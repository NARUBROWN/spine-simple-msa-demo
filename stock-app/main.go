package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/consumer"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/store"
	"github.com/NARUBROWN/spine/pkg/boot"
)

func main() {
	broker := envOrDefault("SPINE_DEMO_KAFKA_BROKER", "localhost:9092")
	groupID := envOrDefault("SPINE_DEMO_STOCK_GROUP", "stock-service")
	statePath := envOrDefault("SPINE_DEMO_STOCK_STATE", ".spine-demo/stock-state.json")
	topicPrefix := os.Getenv("SPINE_DEMO_TOPIC_PREFIX")

	app := spine.New()
	app.Constructor(
		func() *store.IdempotencyStore {
			idempotencyStore, err := store.NewIdempotencyStore(statePath)
			if err != nil {
				panic(fmt.Errorf("initialize idempotency store: %w", err))
			}
			return idempotencyStore
		},
		consumer.NewOrderConsumer,
	)

	if err := app.Consumers().Register(topicPrefix+"order.created", (*consumer.OrderConsumer).OnCreated); err != nil {
		log.Fatalf("Consumer registration failed: %v", err)
	}

	if err := app.Run(boot.Options{
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
