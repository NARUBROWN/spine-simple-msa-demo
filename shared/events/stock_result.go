package events

import "time"

type StockReserved struct {
	EventID       string    `json:"event_id" validate:"required"`
	SourceEventID string    `json:"source_event_id" validate:"required"`
	OrderID       int64     `json:"order_id" validate:"gt=0"`
	Quantity      int       `json:"quantity" validate:"gt=0"`
	At            time.Time `json:"at"`
}

func (s StockReserved) Name() string {
	return "stock.reserved"
}

func (s StockReserved) OccurredAt() time.Time {
	return s.At
}

type StockRejected struct {
	EventID       string    `json:"event_id" validate:"required"`
	SourceEventID string    `json:"source_event_id" validate:"required"`
	OrderID       int64     `json:"order_id" validate:"gt=0"`
	Quantity      int       `json:"quantity" validate:"gt=0"`
	Reason        string    `json:"reason"`
	At            time.Time `json:"at"`
}

func (s StockRejected) Name() string {
	return "stock.rejected"
}

func (s StockRejected) OccurredAt() time.Time {
	return s.At
}
