package service

import (
	"testing"
	"time"
)

func TestAcceptedKeepsCompletedStatusForDuplicateEvent(t *testing.T) {
	tracker := NewOrderTrackerService()
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1"); !accepted {
		t.Fatal("first event was rejected")
	}
	completed := OrderStatus{
		OrderID:   1,
		EventID:   "order-1-normal-v1",
		Status:    "reserved",
		UpdatedAt: time.Now().UTC(),
	}
	if !tracker.Complete("stock-reserved-order-1-normal-v1", completed) {
		t.Fatal("result was not applied")
	}

	status, accepted := tracker.Accepted(1, "order-1-normal-v1")
	if !accepted {
		t.Fatal("duplicate event was rejected")
	}
	if status.Status != "reserved" {
		t.Fatalf("duplicate reset status to %q", status.Status)
	}
}

func TestAcceptedRejectsDifferentEventForExistingOrder(t *testing.T) {
	tracker := NewOrderTrackerService()
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1"); !accepted {
		t.Fatal("first event was rejected")
	}
	if _, accepted := tracker.Accepted(1, "order-1-transient-v1"); accepted {
		t.Fatal("different event reused an existing order id")
	}
}

func TestCompleteRejectsUnknownAndMismatchedSourceEvents(t *testing.T) {
	tracker := NewOrderTrackerService()
	status := OrderStatus{OrderID: 1, EventID: "order-1-normal-v1", Status: "reserved"}
	if tracker.Complete("result-unknown", status) {
		t.Fatal("result for an unknown order was applied")
	}
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1"); !accepted {
		t.Fatal("first event was rejected")
	}
	status.EventID = "order-1-transient-v1"
	if tracker.Complete("result-mismatch", status) {
		t.Fatal("result for a different source event was applied")
	}
}
