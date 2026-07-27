package main

import (
	"context"
	"flag"
	"log"
	"os"
	"time"

	"github.com/NARUBROWN/spine-simple-msa/scenarios/outbox"
)

func main() {
	orderID := flag.Int64("order-id", 0, "order ID to persist and publish")
	quantity := flag.Int("quantity", 1, "stock quantity to reserve")
	statePath := flag.String("state", ".spine-demo/outbox-state.json", "outbox state file")
	flag.Parse()
	if *orderID <= 0 || *quantity <= 0 {
		log.Fatal("order-id and quantity must be positive")
	}

	store, err := outbox.Open(*statePath)
	if err != nil {
		log.Fatalf("Outbox initialization failed: %v", err)
	}
	message, created, err := store.PlaceOrder(*orderID, *quantity)
	if err != nil {
		log.Fatalf("Order transaction failed: %v", err)
	}
	log.Printf("Order transaction completed: event_id=%s created=%t", message.ID, created)

	broker := os.Getenv("SPINE_DEMO_KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}
	publisher := outbox.NewKafkaPublisher(broker, os.Getenv("SPINE_DEMO_TOPIC_PREFIX"))
	defer func() {
		if err := publisher.Close(); err != nil {
			log.Printf("Kafka publisher close failed: %v", err)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	published, err := store.Relay(ctx, publisher)
	if err != nil {
		log.Fatalf("Outbox relay failed: %v", err)
	}
	log.Printf("Outbox relay completed: published=%d", published)
}
