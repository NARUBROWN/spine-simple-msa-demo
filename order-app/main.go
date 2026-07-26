package main

import (
	"log"
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/order-app/controller"
	"github.com/NARUBROWN/spine/pkg/boot"
)

func main() {
	broker := os.Getenv("SPINE_DEMO_KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	app := spine.New()

	app.Constructor(
		controller.NewOrderController,
	)

	app.Route(
		"POST",
		"/orders/:order_id",
		(*controller.OrderController).Create,
	)

	if err := app.Run(boot.Options{
		Address:                ":8080",
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers:                []string{broker},
			AllowInsecureTransport: true,
			Write: &boot.KafkaWriteOptions{
				TopicPrefix: "",
			},
		},
		HTTP: &boot.HTTPOptions{},
	}); err != nil {
		log.Fatalf("Application failed: %v", err)
	}
}
