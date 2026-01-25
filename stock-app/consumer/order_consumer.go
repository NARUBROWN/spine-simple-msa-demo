package consumer

import (
	"context"
	"log"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
)

type OrderConsumer struct{}

func NewOrderConsumer() *OrderConsumer {
	return &OrderConsumer{}
}

func (c *OrderConsumer) OnCreated(ctx context.Context, eventName string, event events.OrderCreated) error {
	log.Println("Kafka Event:", eventName)
	log.Println("OrderID:", event.OrderID)
	return nil
}
