package main

import (
	"log"
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
)

func main() {
	broker := os.Getenv("SPINE_DEMO_KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	app := spine.New()

	app.Constructor(
		consumer.NewOrderConsumer,
	)

	if err := app.Consumers().Register(
		"order.created",
		(*consumer.OrderConsumer).OnCreated,
	); err != nil {
		log.Fatalf("Consumer registration failed: %v", err)
	}

	if err := app.Run(boot.Options{
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers:                []string{broker},
			AllowInsecureTransport: true,
			Read: &boot.KafkaReadOptions{
				GroupID: "stock-service",
			},
		},
	}); err != nil {
		log.Fatalf("Application failed: %v", err)
	}
}
