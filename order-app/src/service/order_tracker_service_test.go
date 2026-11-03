package service

import (
	"testing"
	"time"
)

func TestAcceptedKeepsCompletedStatusForDuplicateEvent(t *testing.T) {
	tracker := NewOrderTrackerService()
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1); !accepted {
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

	status, accepted := tracker.Accepted(1, "order-1-normal-v1", 1)
	if !accepted {
		t.Fatal("duplicate event was rejected")
	}
	if status.Status != "reserved" {
		t.Fatalf("duplicate reset status to %q", status.Status)
	}
}

func TestAcceptedRejectsDifferentEventForExistingOrder(t *testing.T) {
	tracker := NewOrderTrackerService()
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1); !accepted {
		t.Fatal("first event was rejected")
	}
	if _, accepted := tracker.Accepted(1, "order-1-transient-v1", 1); accepted {
		t.Fatal("different event reused an existing order id")
	}
}

func TestAcceptedRejectsChangedQuantityForExistingEvent(t *testing.T) {
	tracker := NewOrderTrackerService()
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1); !accepted {
		t.Fatal("first event was rejected")
	}
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1000); accepted {
		t.Fatal("same event ID with a different quantity was accepted")
	}
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1); !accepted {
		t.Fatal("rejected conflict must not change the original request")
	}
}

func TestCompleteRejectsUnknownAndMismatchedSourceEvents(t *testing.T) {
	tracker := NewOrderTrackerService()
	status := OrderStatus{OrderID: 1, EventID: "order-1-normal-v1", Status: "reserved"}
	if tracker.Complete("result-unknown", status) {
		t.Fatal("result for an unknown order was applied")
	}
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1); !accepted {
		t.Fatal("first event was rejected")
	}
	status.EventID = "order-1-transient-v1"
	if tracker.Complete("result-mismatch", status) {
		t.Fatal("result for a different source event was applied")
	}
}

func TestCompleteKeepsFirstTerminalDecision(t *testing.T) {
	tracker := NewOrderTrackerService()
	if _, accepted := tracker.Accepted(1, "order-1-normal-v1", 1); !accepted {
		t.Fatal("first event was rejected")
	}
	status := OrderStatus{OrderID: 1, EventID: "order-1-normal-v1", Status: "reserved"}
	if tracker.Complete("", status) {
		t.Fatal("result without an event ID was applied")
	}
	if !tracker.Complete("stock-reserved-order-1-normal-v1", status) {
		t.Fatal("first result was not applied")
	}
	status.Status = "rejected"
	if tracker.Complete("stock-rejected-order-1-normal-v1", status) {
		t.Fatal("contradictory result replaced the terminal decision")
	}
	got, _ := tracker.Get(1)
	if got.Status != "reserved" {
		t.Fatalf("status = %q, want reserved", got.Status)
	}
}
