package order

import (
	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/src/service"
	spinectx "github.com/NARUBROWN/spine/pkg/spine"
)

// OrderConsumer는 구독과 DTO 바인딩을 재고 예약 서비스에 연결합니다.
type OrderConsumer struct{ reservations *service.ReservationService }

func (*OrderConsumer) Constructor(reservations *service.ReservationService) *OrderConsumer {
	return &OrderConsumer{reservations: reservations}
}
func (c *OrderConsumer) OnCreated(ctx spinectx.Ctx, eventName string, event events.OrderCreated) error {
	return c.reservations.Reserve(ctx.ExecutionContext().Context(), eventName, event)
}

// Register는 주문 생성 토픽과 수신 메서드를 연결합니다.
func Register(app spine.App) error {
	return app.Consumers().Register("order.created", (*OrderConsumer).OnCreated)
}
