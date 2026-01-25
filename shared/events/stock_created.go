package events

import "time"

type StockCreated struct {
	OrderID int64     `json:"order_id"`
	At      time.Time `json:"at"`
}

func (o *StockCreated) Name() string {
	return "stock.created"
}

func (o *StockCreated) OccurredAt() time.Time {
	return o.At
}
