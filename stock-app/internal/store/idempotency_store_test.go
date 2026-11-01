package store

import (
	"path/filepath"
	"testing"
	"time"
)

func TestIdempotencyStorePersistsAttemptsAndDecision(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	first, err := NewIdempotencyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := first.Attempt("event-1")
	if err != nil || attempt != 1 {
		t.Fatalf("first attempt = %d, %v", attempt, err)
	}
	want := Decision{EventID: "event-1", OrderID: 7, Quantity: 1, Outcome: "reserved", At: time.Now().UTC()}
	if err := first.Save(want); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewIdempotencyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := reopened.Decision("event-1")
	if !ok || got.Outcome != want.Outcome || got.OrderID != want.OrderID {
		t.Fatalf("persisted decision = %+v, exists=%t", got, ok)
	}
	attempt, err = reopened.Attempt("event-1")
	if err != nil || attempt != 2 {
		t.Fatalf("reopened attempt = %d, %v", attempt, err)
	}
}
