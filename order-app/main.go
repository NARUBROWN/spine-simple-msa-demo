package main

import (
	"log"
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/order-app/consumer"
	"github.com/NARUBROWN/spine-simple-msa/order-app/controller"
	"github.com/NARUBROWN/spine-simple-msa/order-app/state"
	"github.com/NARUBROWN/spine/pkg/boot"
)

func main() {
	broker := envOrDefault("SPINE_DEMO_KAFKA_BROKER", "localhost:9092")
	address := envOrDefault("SPINE_DEMO_ORDER_ADDRESS", ":8080")
	groupID := envOrDefault("SPINE_DEMO_ORDER_GROUP", "order-service")
	topicPrefix := os.Getenv("SPINE_DEMO_TOPIC_PREFIX")

	app := spine.New()
	app.Constructor(
		state.NewOrderTracker,
		controller.NewOrderController,
		consumer.NewStockConsumer,
	)
	app.Route("POST", "/orders/:order_id", (*controller.OrderController).Create)
	app.Route("POST", "/orders/:order_id/large", (*controller.OrderController).CreateLarge)
	app.Route("POST", "/scenarios/transient/:order_id", (*controller.OrderController).CreateTransient)
	app.Route("POST", "/scenarios/poison/:order_id", (*controller.OrderController).CreatePoison)
	app.Route("GET", "/orders/:order_id/status", (*controller.OrderController).Status)

	if err := app.Consumers().Register(topicPrefix+"stock.reserved", (*consumer.StockConsumer).OnReserved); err != nil {
		log.Fatalf("Consumer registration failed: %v", err)
	}
	if err := app.Consumers().Register(topicPrefix+"stock.rejected", (*consumer.StockConsumer).OnRejected); err != nil {
		log.Fatalf("Consumer registration failed: %v", err)
	}

	if err := app.Run(boot.Options{
		Address:                address,
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers:                []string{broker},
			AllowInsecureTransport: true,
			ConsumerRetry: boot.ConsumerRetryOptions{
				InitialDelay: 200 * time.Millisecond,
				MaxDelay:     2 * time.Second,
				Multiplier:   2,
				Jitter:       0.2,
			},
			Read:  &boot.KafkaReadOptions{GroupID: groupID},
			Write: &boot.KafkaWriteOptions{TopicPrefix: topicPrefix},
		},
		HTTP: &boot.HTTPOptions{},
	}); err != nil {
		log.Fatalf("Application failed: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
