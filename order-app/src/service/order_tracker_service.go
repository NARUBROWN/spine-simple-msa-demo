package service

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

type OrderTrackerService struct {
	mu         sync.RWMutex
	orders     map[int64]OrderStatus
	quantities map[int64]int
	results    map[string]struct{}
}

func (*OrderTrackerService) Constructor() *OrderTrackerService {
	return &OrderTrackerService{
		orders:     make(map[int64]OrderStatus),
		quantities: make(map[int64]int),
		results:    make(map[string]struct{}),
	}
}

func (t *OrderTrackerService) Accepted(orderID int64, eventID string, quantity int) (OrderStatus, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if current, exists := t.orders[orderID]; exists {
		return current, current.EventID == eventID && t.quantities[orderID] == quantity
	}
	status := OrderStatus{OrderID: orderID, EventID: eventID, Status: "pending", UpdatedAt: time.Now().UTC()}
	t.orders[orderID] = status
	t.quantities[orderID] = quantity
	return status, true
}

func (t *OrderTrackerService) Complete(resultEventID string, status OrderStatus) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if resultEventID == "" || (status.Status != "reserved" && status.Status != "rejected") {
		return false
	}
	if _, exists := t.results[resultEventID]; exists {
		return false
	}
	current, exists := t.orders[status.OrderID]
	if !exists || current.EventID != status.EventID || current.Status != "pending" {
		return false
	}
	t.results[resultEventID] = struct{}{}
	t.orders[status.OrderID] = status
	return true
}

func (t *OrderTrackerService) Get(orderID int64) (OrderStatus, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	status, ok := t.orders[orderID]
	return status, ok
}
