package main

import (
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
)

func main() {
	app := spine.New()

	app.Constructor(
		consumer.NewOrderConsumer,
	)

	app.Consumers().Register(
		"order.created",
		(*consumer.OrderConsumer).OnCreated,
	)

	app.Run(boot.Options{
		Address:                ":8081",
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers: []string{"localhost:9092"},
			Read: &boot.KafkaReadOptions{
				GroupID: "stock-service",
			},
		},
	})
}
