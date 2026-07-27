package events

import "testing"

func TestNewOrderCreatedBuildsStableEventID(t *testing.T) {
	first := NewOrderCreated(42, 1, ScenarioNormal, 0)
	second := NewOrderCreated(42, 1, ScenarioNormal, 0)
	if first.EventID != second.EventID || first.EventID != "order-42-normal-v1" {
		t.Fatalf("event ID is not stable: first=%q second=%q", first.EventID, second.EventID)
	}
}
