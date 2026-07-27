package consumer

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/store"
	"github.com/NARUBROWN/spine/pkg/event/publish"
)

type recordingBus struct {
	events []publish.DomainEvent
}

func (b *recordingBus) Publish(events ...publish.DomainEvent) {
	b.events = append(b.events, events...)
}

func TestOrderConsumerRetriesThenDeduplicates(t *testing.T) {
	idempotencyStore, err := store.NewIdempotencyStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	consumer := NewOrderConsumer(idempotencyStore)
	bus := &recordingBus{}
	ctx := context.WithValue(context.Background(), publish.PublisherKey, bus)
	event := events.NewOrderCreated(10, 1, events.ScenarioTransient, 1)

	if err := consumer.OnCreated(ctx, event.Name(), event); err == nil {
		t.Fatal("first transient attempt must fail")
	}
	if err := consumer.OnCreated(ctx, event.Name(), event); err != nil {
		t.Fatalf("second transient attempt failed: %v", err)
	}
	if err := consumer.OnCreated(ctx, event.Name(), event); err != nil {
		t.Fatalf("duplicate delivery failed: %v", err)
	}
	if len(bus.events) != 2 {
		t.Fatalf("published results = %d, want one original and one duplicate result", len(bus.events))
	}
	if _, ok := bus.events[0].(events.StockReserved); !ok {
		t.Fatalf("published event type = %T", bus.events[0])
	}
}

func TestOrderConsumerRejectsPoisonEvent(t *testing.T) {
	idempotencyStore, err := store.NewIdempotencyStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	consumer := NewOrderConsumer(idempotencyStore)
	event := events.NewOrderCreated(11, 1, events.ScenarioPoison, 0)
	if err := consumer.OnCreated(context.Background(), event.Name(), event); err == nil {
		t.Fatal("poison event must fail without being acknowledged")
	}
	if _, ok := idempotencyStore.Decision(event.EventID); ok {
		t.Fatal("poison event must not create a stock decision")
	}
}

func TestOrderConsumerPublishesRejection(t *testing.T) {
	idempotencyStore, err := store.NewIdempotencyStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	consumer := NewOrderConsumer(idempotencyStore)
	bus := &recordingBus{}
	ctx := context.WithValue(context.Background(), publish.PublisherKey, bus)
	event := events.NewOrderCreated(12, 1000, events.ScenarioNormal, 0)
	if err := consumer.OnCreated(ctx, event.Name(), event); err != nil {
		t.Fatal(err)
	}
	if len(bus.events) != 1 {
		t.Fatalf("published results = %d", len(bus.events))
	}
	if _, ok := bus.events[0].(events.StockRejected); !ok {
		t.Fatalf("published event type = %T", bus.events[0])
	}
}
