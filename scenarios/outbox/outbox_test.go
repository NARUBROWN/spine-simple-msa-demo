package outbox

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

type scriptedPublisher struct {
	fail  bool
	calls int
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
