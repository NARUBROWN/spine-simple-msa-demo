package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/event/publish"
)

var ErrInvalidOrder = errors.New("order_id and quantity must be positive")
var ErrOrderConflict = errors.New("order_id already belongs to a different request")

type OrderService struct{ tracker *OrderTrackerService }

func (*OrderService) Constructor(tracker *OrderTrackerService) *OrderService {
	return &OrderService{tracker: tracker}
}

// Place는 주문을 접수하고 후처리에서 발행할 도메인 이벤트를 기록합니다.
// Kafka writer, HTTP 응답, 브로커의 ACK는 이 서비스의 책임이 아닙니다.
func (s *OrderService) Place(ctx context.Context, orderID int64, quantity int, scenario string, failuresBeforeSuccess int) (OrderStatus, error) {
	if orderID <= 0 || quantity <= 0 {
		return OrderStatus{}, ErrInvalidOrder
	}
	event := events.NewOrderCreated(orderID, quantity, scenario, failuresBeforeSuccess)
	fields := publish.Fields{
		"event_id": event.EventID,
		"order_id": event.OrderID,
		"quantity": event.Quantity,
		"scenario": event.Scenario,
		"at":       event.At,
	}
	if event.FailuresBeforeSuccess != 0 {
		fields["failures_before_success"] = event.FailuresBeforeSuccess
	}
	status, accepted, err := s.tracker.accept(orderID, event.EventID, quantity, func() error {
		return publish.Event(ctx, events.OrderCreatedDestination, fields)
	})
	if err != nil {
		return OrderStatus{}, fmt.Errorf("queue order event: %w", err)
	}
	if !accepted {
		return OrderStatus{}, ErrOrderConflict
	}
	return status, nil
}
func (s *OrderService) Status(orderID int64) (OrderStatus, bool) { return s.tracker.Get(orderID) }
