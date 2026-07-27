//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type synchronizedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type childProcess struct {
	cmd    *exec.Cmd
	logs   *synchronizedBuffer
	done   chan error
	exited chan struct{}
}

func TestKafkaRoundTripDeliveryContracts(t *testing.T) {
	broker := os.Getenv("SPINE_DEMO_E2E_BROKER")
	if broker == "" {
		t.Skip("SPINE_DEMO_E2E_BROKER is not set")
	}
	repositoryRoot, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	temporaryDirectory := t.TempDir()
	orderBinary := filepath.Join(temporaryDirectory, "order-app")
	stockBinary := filepath.Join(temporaryDirectory, "stock-app")
	build(t, repositoryRoot, orderBinary, "./order-app")
	build(t, repositoryRoot, stockBinary, "./stock-app")

	t.Run("consumer waits for broker before becoming ready", func(t *testing.T) {
		unavailableAddress := unusedAddress(t)
		process := start(t, repositoryRoot, stockBinary,
			"SPINE_DEMO_KAFKA_BROKER="+unavailableAddress,
			"SPINE_DEMO_STOCK_GROUP=e2e-readiness",
			"SPINE_DEMO_STOCK_STATE="+filepath.Join(temporaryDirectory, "readiness.json"),
		)
		time.Sleep(700 * time.Millisecond)
		select {
		case <-process.exited:
			t.Fatalf("consumer exited instead of retrying startup: %v\n%s", <-process.done, process.logs.String())
		default:
		}
		if strings.Contains(process.logs.String(), "Starting consumer for topic") {
			t.Fatalf("consumer became ready without a broker:\n%s", process.logs.String())
		}
		stop(t, process)
	})

	port := unusedPort(t)
	unique := fmt.Sprintf("%d", time.Now().UnixNano())
	topicPrefix := "e2e." + unique + "."
	stock := start(t, repositoryRoot, stockBinary,
		"SPINE_DEMO_KAFKA_BROKER="+broker,
		"SPINE_DEMO_STOCK_GROUP=e2e-stock-"+unique,
		"SPINE_DEMO_STOCK_STATE="+filepath.Join(temporaryDirectory, "stock.json"),
		"SPINE_DEMO_TOPIC_PREFIX="+topicPrefix,
	)
	t.Cleanup(func() { stop(t, stock) })
	waitForLog(t, stock.logs, "Starting consumer for topic '"+topicPrefix+"order.created'", 15*time.Second)

	order := start(t, repositoryRoot, orderBinary,
		"SPINE_DEMO_KAFKA_BROKER="+broker,
		"SPINE_DEMO_ORDER_GROUP=e2e-order-"+unique,
		fmt.Sprintf("SPINE_DEMO_ORDER_ADDRESS=127.0.0.1:%d", port),
		"SPINE_DEMO_TOPIC_PREFIX="+topicPrefix,
	)
	t.Cleanup(func() { stop(t, order) })
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("order logs:\n%s\nstock logs:\n%s", order.logs.String(), stock.logs.String())
		}
	})
	baseURL := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitForHTTP(t, baseURL+"/orders/999/status", 15*time.Second)

	// 새 consumer group은 첫 할당 직전의 메시지를 놓칠 수 있으므로, 동일한 안정적
	// event_id를 재발행하면서 양방향 consumer가 실제로 처리할 준비가 됐는지 확인합니다.
	postUntilStatus(t, baseURL, 100, "reserved", 15*time.Second)

	post(t, baseURL+"/orders/101", http.StatusAccepted)
	waitForStatus(t, baseURL, 101, "reserved", 15*time.Second)

	post(t, baseURL+"/orders/102/large", http.StatusAccepted)
	waitForStatus(t, baseURL, 102, "rejected", 15*time.Second)

	post(t, baseURL+"/scenarios/transient/103", http.StatusAccepted)
	waitForStatus(t, baseURL, 103, "reserved", 15*time.Second)
	waitForLog(t, stock.logs, "Transient processing failure", 10*time.Second)
	waitForLog(t, stock.logs, "order_id=103 outcome=reserved attempt=2", 10*time.Second)

	post(t, baseURL+"/orders/101", http.StatusAccepted)
	waitForLog(t, stock.logs, "Duplicate event detected", 10*time.Second)
	waitForStatus(t, baseURL, 101, "reserved", 5*time.Second)
	post(t, baseURL+"/scenarios/transient/101", http.StatusConflict)
	waitForStatus(t, baseURL, 101, "reserved", 5*time.Second)

	post(t, baseURL+"/scenarios/poison/104", http.StatusAccepted)
	waitForOccurrences(t, stock.logs, "Poison event rejected", 2, 15*time.Second)

	stop(t, order)
	stop(t, stock)
	if !strings.Contains(order.logs.String(), "All consumers stopped") {
		t.Fatalf("order consumer did not drain:\n%s", order.logs.String())
	}
	if !strings.Contains(stock.logs.String(), "All consumers stopped") {
		t.Fatalf("stock consumer did not drain:\n%s", stock.logs.String())
	}
}

func postUntilStatus(t *testing.T, baseURL string, orderID int64, expected string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("%s/orders/%d", baseURL, orderID)
	for time.Now().Before(deadline) {
		response, err := http.Post(url, "application/json", nil)
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusAccepted {
				t.Fatalf("POST %s status=%d", url, response.StatusCode)
			}
		}
		if currentStatus(baseURL, orderID) == expected {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("order %d did not reach %s while probing consumer readiness", orderID, expected)
}

func currentStatus(baseURL string, orderID int64) string {
	response, err := http.Get(fmt.Sprintf("%s/orders/%d/status", baseURL, orderID))
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	var status struct {
		Status string `json:"status"`
	}
	if json.NewDecoder(response.Body).Decode(&status) != nil {
		return ""
	}
	return status.Status
}

func build(t *testing.T, directory, output, packagePath string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", output, packagePath)
	cmd.Dir = directory
	cmd.Env = os.Environ()
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", packagePath, err, output)
	}
}

func start(t *testing.T, directory, binary string, environment ...string) *childProcess {
	t.Helper()
	logs := &synchronizedBuffer{}
	cmd := exec.Command(binary)
	cmd.Dir = directory
	cmd.Env = append(os.Environ(), environment...)
	cmd.Stdout = logs
	cmd.Stderr = logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		done <- cmd.Wait()
		close(exited)
	}()
	return &childProcess{cmd: cmd, logs: logs, done: done, exited: exited}
}

func stop(t *testing.T, process *childProcess) {
	t.Helper()
	if process == nil {
		return
	}
	select {
	case <-process.exited:
		return
	default:
	}
	if err := process.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Logf("interrupt process: %v", err)
	}
	select {
	case err := <-process.done:
		if err != nil {
			t.Logf("process exit: %v\n%s", err, process.logs.String())
		}
	case <-time.After(12 * time.Second):
		_ = process.cmd.Process.Kill()
		<-process.done
		t.Fatalf("process did not stop gracefully:\n%s", process.logs.String())
	}
}

func post(t *testing.T, url string, expectedStatus int) {
	t.Helper()
	response, err := http.Post(url, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("POST %s status=%d body=%s", url, response.StatusCode, body)
	}
}

func waitForHTTP(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := http.Get(url)
		if err == nil {
			response.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("HTTP listener did not become ready: %s", url)
}

func waitForStatus(t *testing.T, baseURL string, orderID int64, expected string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("%s/orders/%d/status", baseURL, orderID)
	for time.Now().Before(deadline) {
		response, err := http.Get(url)
		if err == nil {
			var status struct {
				Status string `json:"status"`
			}
			decodeErr := json.NewDecoder(response.Body).Decode(&status)
			response.Body.Close()
			if decodeErr == nil && status.Status == expected {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("order %d did not reach %s", orderID, expected)
}

func waitForLog(t *testing.T, logs *synchronizedBuffer, text string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), text) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("log %q not found:\n%s", text, logs.String())
}

func waitForOccurrences(t *testing.T, logs *synchronizedBuffer, text string, count int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Count(logs.String(), text) >= count {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("log %q occurred fewer than %d times:\n%s", text, count, logs.String())
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("127.0.0.1:%d", unusedPort(t))
}

func unusedPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
