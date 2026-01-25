package main

import (
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/order-app/controller"
	"github.com/NARUBROWN/spine/pkg/boot"
)

func main() {
	app := spine.New()

	app.Constructor(
		controller.NewOrderController,
	)

	app.Route(
		"POST",
		"/orders/:order_id",
		(*controller.OrderController).Create,
	)

	app.Run(boot.Options{
		Address:                ":8080",
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers: []string{"localhost:9092"},
			Write: &boot.KafkaWriteOptions{
				TopicPrefix: "",
			},
		},
	})
}
