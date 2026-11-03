package service

import (
	"context"
	"errors"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/event/publish"
)

var ErrInvalidOrder = errors.New("order_id and quantity must be positive")
var ErrOrderConflict = errors.New("order_id already belongs to a different request")

type OrderService struct{ tracker *OrderTrackerService }

func NewOrderService(tracker *OrderTrackerService) *OrderService {
	return &OrderService{tracker: tracker}
}

// Place는 주문을 접수하고 후처리에서 발행할 도메인 이벤트를 기록합니다.
// Kafka writer, HTTP 응답, 브로커의 ACK는 이 서비스의 책임이 아닙니다.
func (s *OrderService) Place(ctx context.Context, orderID int64, quantity int, scenario string, failuresBeforeSuccess int) (OrderStatus, error) {
	if orderID <= 0 || quantity <= 0 {
		return OrderStatus{}, ErrInvalidOrder
	}
	event := events.NewOrderCreated(orderID, quantity, scenario, failuresBeforeSuccess)
	status, accepted := s.tracker.Accepted(orderID, event.EventID, quantity)
	if !accepted {
		return OrderStatus{}, ErrOrderConflict
	}
	publish.Event(ctx, event)
	return status, nil
}
func (s *OrderService) Status(orderID int64) (OrderStatus, bool) { return s.tracker.Get(orderID) }
