package outbox

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type scriptedPublisher struct {
	fail  bool
	calls int
}

type recordingPublisher struct {
	payloads [][]byte
}

func (p *recordingPublisher) Publish(_ context.Context, _ string, payload []byte) error {
	p.payloads = append(p.payloads, append([]byte(nil), payload...))
	return nil
}

// 전송 후 영구 표시에 실패하면 재시작 후 동일한 이벤트가 다시 전송될 수 있습니다.
func TestRelayReplaysSameMessageWhenPublishedMarkerCannotBeSaved(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "outbox.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := store.PlaceOrder(42, 3); err != nil || !created {
		t.Fatalf("place order: created=%t err=%v", created, err)
	}
	blockingFile := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blockingFile, []byte("block"), 0o600); err != nil {
		t.Fatal(err)
	}
	store.path = filepath.Join(blockingFile, "outbox.json")
	publisher := &recordingPublisher{}
	if count, err := store.Relay(context.Background(), publisher); err == nil || count != 0 || len(publisher.payloads) != 1 {
		t.Fatalf("expected publish success followed by marker failure: count=%d calls=%d err=%v", count, len(publisher.payloads), err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := reopened.Relay(context.Background(), publisher); err != nil || count != 1 {
		t.Fatalf("retry pending message: count=%d err=%v", count, err)
	}
	if len(publisher.payloads) != 2 || !reflect.DeepEqual(publisher.payloads[0], publisher.payloads[1]) {
		t.Fatal("retry must publish the same message")
	}
	if count, err := reopened.Relay(context.Background(), publisher); err != nil || count != 0 || len(publisher.payloads) != 2 {
		t.Fatalf("published marker must prevent another relay: count=%d calls=%d err=%v", count, len(publisher.payloads), err)
	}
}

func (p *scriptedPublisher) Publish(context.Context, string, []byte) error {
	p.calls++
	if p.fail {
		return errors.New("broker unavailable")
	}
	return nil
}

func TestStoreRecoversPendingOutboxAfterPublishFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	message, created, err := store.PlaceOrder(42, 3)
	if err != nil || !created {
		t.Fatalf("place order: created=%t err=%v", created, err)
	}
	if message.ID != "outbox-order-42-v1" {
		t.Fatalf("message ID = %q", message.ID)
	}

	failing := &scriptedPublisher{fail: true}
	if published, err := store.Relay(context.Background(), failing); err == nil || published != 0 {
		t.Fatalf("failed relay: published=%d err=%v", published, err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	success := &scriptedPublisher{}
	if published, err := reopened.Relay(context.Background(), success); err != nil || published != 1 {
		t.Fatalf("recovered relay: published=%d err=%v", published, err)
	}
	if published, err := reopened.Relay(context.Background(), success); err != nil || published != 0 {
		t.Fatalf("second relay: published=%d err=%v", published, err)
	}
	if success.calls != 1 {
		t.Fatalf("successful publisher calls = %d", success.calls)
	}
}
