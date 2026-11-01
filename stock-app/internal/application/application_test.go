package application

import (
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
	registrations := app.Consumers().Registrations()
	if len(registrations) != 1 || registrations[0].Topic != "test.order.created" || options.Kafka.Write.TopicPrefix != "test." {
		t.Fatalf("consumer/publisher prefix mismatch: %#v / %#v", registrations, options.Kafka.Write)
	}
}
