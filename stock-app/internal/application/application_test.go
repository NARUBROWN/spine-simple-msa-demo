package application

import (
	"github.com/NARUBROWN/spine"
	"os"
	"path/filepath"
	"testing"
)

func TestBuildApplicationDoesNotReadStateOrConnectToBroker(t *testing.T) {
	statePath := filepath.Join(t.TempDir(), "invalid-state.json")
	if err := os.WriteFile(statePath, []byte("invalid JSON"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SPINE_DEMO_STOCK_STATE", statePath)
	t.Setenv("SPINE_DEMO_KAFKA_BROKER", "127.0.0.1:1")
	t.Setenv("SPINE_DEMO_TOPIC_PREFIX", "test.")
	app, options, err := BuildApplication()
	if err != nil {
		t.Fatal(err)
	}
	if err := app.Validate(options); err != nil {
		t.Fatal(err)
	}
	if options.HTTP != nil {
		t.Fatal("재고 워커에 HTTP가 활성화되었습니다")
	}
	description, err := spine.DescribeRegistrations(app)
	if err != nil {
		t.Fatal(err)
	}
	registrations := description.Consumers
	for _, registration := range registrations {
		if registration.Topic == "order.created" && registration.Handler.Method == "OnCreated" {
			return
		}
	}
	t.Fatalf("order.created consumer is missing: %#v", registrations)
}
