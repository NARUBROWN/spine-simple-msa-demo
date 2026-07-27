package consumer

import (
	"context"
	"log"

	"github.com/NARUBROWN/spine-simple-msa/order-app/state"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
)

type StockConsumer struct {
	tracker *state.OrderTracker
}

func NewStockConsumer(tracker *state.OrderTracker) *StockConsumer {
	return &StockConsumer{tracker: tracker}
}

func (c *StockConsumer) OnReserved(_ context.Context, eventName string, event events.StockReserved) error {
	updated := c.tracker.Complete(event.EventID, state.OrderStatus{
		OrderID:   event.OrderID,
		EventID:   event.SourceEventID,
		Status:    "reserved",
		UpdatedAt: event.At,
	})
	log.Printf("Stock result received: event=%s event_id=%s order_id=%d status=reserved duplicate=%t", eventName, event.EventID, event.OrderID, !updated)
	return nil
}

func (c *StockConsumer) OnRejected(_ context.Context, eventName string, event events.StockRejected) error {
	updated := c.tracker.Complete(event.EventID, state.OrderStatus{
		OrderID:   event.OrderID,
		EventID:   event.SourceEventID,
		Status:    "rejected",
		Reason:    event.Reason,
		UpdatedAt: event.At,
	})
	log.Printf("Stock result received: event=%s event_id=%s order_id=%d status=rejected duplicate=%t", eventName, event.EventID, event.OrderID, !updated)
	return nil
}
