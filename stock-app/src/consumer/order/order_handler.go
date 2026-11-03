package order

import (
	"context"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/src/service"
)

// OrderConsumer는 구독과 DTO 바인딩을 재고 예약 서비스에 연결합니다.
type OrderConsumer struct{ reservations *service.ReservationService }

func NewOrderConsumer(reservations *service.ReservationService) *OrderConsumer {
	return &OrderConsumer{reservations: reservations}
}
func (c *OrderConsumer) OnCreated(ctx context.Context, eventName string, event events.OrderCreated) error {
	return c.reservations.Reserve(ctx, eventName, event)
}
