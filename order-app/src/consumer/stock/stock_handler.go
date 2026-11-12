package stock

import (
	"log"

	"github.com/NARUBROWN/spine-simple-msa/order-app/src/service"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	spinectx "github.com/NARUBROWN/spine/pkg/spine"
)

type StockConsumer struct {
	tracker *service.OrderTrackerService
}

func (*StockConsumer) Constructor(tracker *service.OrderTrackerService) *StockConsumer {
	return &StockConsumer{tracker: tracker}
}

func (c *StockConsumer) OnReserved(_ spinectx.Ctx, eventName string, event events.StockReserved) error {
	updated := c.tracker.Complete(event.EventID, service.OrderStatus{
		OrderID:   event.OrderID,
		EventID:   event.SourceEventID,
		Status:    "reserved",
		UpdatedAt: event.At,
	})
	log.Printf("Stock result received: event=%s event_id=%s order_id=%d status=reserved duplicate=%t", eventName, event.EventID, event.OrderID, !updated)
	return nil
}

func (c *StockConsumer) OnRejected(_ spinectx.Ctx, eventName string, event events.StockRejected) error {
	updated := c.tracker.Complete(event.EventID, service.OrderStatus{
		OrderID:   event.OrderID,
		EventID:   event.SourceEventID,
		Status:    "rejected",
		Reason:    event.Reason,
		UpdatedAt: event.At,
	})
	log.Printf("Stock result received: event=%s event_id=%s order_id=%d status=rejected duplicate=%t", eventName, event.EventID, event.OrderID, !updated)
	return nil
}
