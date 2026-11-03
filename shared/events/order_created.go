package events

import (
	"fmt"
	"time"
)

const (
	ScenarioNormal    = "normal"
	ScenarioTransient = "transient"
	ScenarioPoison    = "poison"
)

type OrderCreated struct {
	EventID               string    `json:"event_id" validate:"required"`
	OrderID               int64     `json:"order_id" validate:"gt=0"`
	Quantity              int       `json:"quantity" validate:"gt=0"`
	Scenario              string    `json:"scenario"`
	FailuresBeforeSuccess int       `json:"failures_before_success,omitempty"`
	At                    time.Time `json:"at"`
}

func NewOrderCreated(orderID int64, quantity int, scenario string, failuresBeforeSuccess int) OrderCreated {
	return OrderCreated{
		// 이 데모에서는 주문 하나가 하나의 생성 이벤트를 뜻하므로 주문 ID와 시나리오로 재현 가능한 키를 만듭니다.
		EventID:               fmt.Sprintf("order-%d-%s-v1", orderID, scenario),
		OrderID:               orderID,
		Quantity:              quantity,
		Scenario:              scenario,
		FailuresBeforeSuccess: failuresBeforeSuccess,
		At:                    time.Now().UTC(),
	}
}

func (o OrderCreated) Name() string {
	return "order.created"
}

func (o OrderCreated) OccurredAt() time.Time {
	return o.At
}
