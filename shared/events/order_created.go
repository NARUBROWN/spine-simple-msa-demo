package events

import "time"

type OrderCreated struct {
	OrderID int64     `json:"order_id"`
	At      time.Time `json:"at"`
}

func (o *OrderCreated) Name() string {
	return "order.created"
}

func (o *OrderCreated) OccurredAt() time.Time {
	return o.At
}
