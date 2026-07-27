package state

import (
	"sync"
	"time"
)

type OrderStatus struct {
	OrderID   int64     `json:"order_id"`
	EventID   string    `json:"event_id"`
	Status    string    `json:"status"`
	Reason    string    `json:"reason,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type OrderTracker struct {
	mu      sync.RWMutex
	orders  map[int64]OrderStatus
	results map[string]struct{}
}

func NewOrderTracker() *OrderTracker {
	return &OrderTracker{
		orders:  make(map[int64]OrderStatus),
		results: make(map[string]struct{}),
	}
}

func (t *OrderTracker) Accepted(orderID int64, eventID string) (OrderStatus, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if current, exists := t.orders[orderID]; exists {
		return current, current.EventID == eventID
	}
	status := OrderStatus{OrderID: orderID, EventID: eventID, Status: "pending", UpdatedAt: time.Now().UTC()}
	t.orders[orderID] = status
	return status, true
}

func (t *OrderTracker) Complete(resultEventID string, status OrderStatus) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.results[resultEventID]; exists {
		return false
	}
	current, exists := t.orders[status.OrderID]
	if !exists || current.EventID != status.EventID {
		return false
	}
	t.results[resultEventID] = struct{}{}
	t.orders[status.OrderID] = status
	return true
}

func (t *OrderTracker) Get(orderID int64) (OrderStatus, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	status, ok := t.orders[orderID]
	return status, ok
}
