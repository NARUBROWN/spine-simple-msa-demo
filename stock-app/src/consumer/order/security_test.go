package order

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/internal/store"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/src/service"
	"github.com/NARUBROWN/spine/pkg/event/publish"
)

func TestMalformedOrderDoesNotCreateDecisionOrPublish(t *testing.T) {
	valid := events.NewOrderCreated(42, 1, events.ScenarioNormal, 0)
	cases := map[string]func(*events.OrderCreated){
		"missing event ID": func(e *events.OrderCreated) { e.EventID = "" },
		"invalid order ID": func(e *events.OrderCreated) { e.OrderID = 0 },
		"invalid quantity": func(e *events.OrderCreated) { e.Quantity = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			state, err := store.NewIdempotencyStore(path)
			if err != nil {
				t.Fatal(err)
			}
			event := valid
			mutate(&event)
			bus := &recordingBus{}
			ctx := publish.WithPublisher(context.Background(), bus)
			if err := NewOrderConsumer(service.NewReservationService(state)).OnCreated(ctx, event.Name(), event); err == nil {
				t.Fatal("invalid event must be rejected")
			}
			if _, exists := state.Decision(event.EventID); exists || len(bus.events) != 0 {
				t.Fatalf("invalid event created a decision or published a result: exists=%t events=%d", exists, len(bus.events))
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid event must not persist an attempt: stat err=%v", err)
			}
		})
	}
}

func TestDuplicateAfterRestartReplaysStoredDecisionAndRejectsConflictingPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state, err := store.NewIdempotencyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	event := events.NewOrderCreated(42, 1, events.ScenarioNormal, 0)
	first := &recordingBus{}
	if err := NewOrderConsumer(service.NewReservationService(state)).OnCreated(publish.WithPublisher(context.Background(), first), event.Name(), event); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.NewIdempotencyStore(path)
	if err != nil {
		t.Fatal(err)
	}
	replay := &recordingBus{}
	consumer := NewOrderConsumer(service.NewReservationService(reopened))
	ctx := publish.WithPublisher(context.Background(), replay)
	if err := consumer.OnCreated(ctx, event.Name(), event); err != nil {
		t.Fatal(err)
	}
	if len(first.events) != 1 || len(replay.events) != 1 || !reflect.DeepEqual(first.events[0], replay.events[0]) {
		t.Fatalf("replay must publish the original result: first=%v replay=%v", first.events, replay.events)
	}
	for _, mutate := range []func(*events.OrderCreated){
		func(e *events.OrderCreated) { e.OrderID++ },
		func(e *events.OrderCreated) { e.Quantity = 1000 },
	} {
		conflict := event
		mutate(&conflict)
		if err := consumer.OnCreated(ctx, conflict.Name(), conflict); err == nil {
			t.Fatal("same event ID with a different order or quantity must fail")
		}
	}
	if len(replay.events) != 1 {
		t.Fatalf("conflicting payload published a result: %v", replay.events)
	}
}

func TestConcurrentConflictingDeliveriesPublishOnlyStoredDecision(t *testing.T) {
	state, err := store.NewIdempotencyStore(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	consumer := NewOrderConsumer(service.NewReservationService(state))
	event := events.NewOrderCreated(42, 1, events.ScenarioNormal, 0)
	type result struct {
		quantity int
		events   []publish.DomainEvent
		err      error
	}
	const deliveries = 16
	results := make(chan result, deliveries)
	var wg sync.WaitGroup
	for i := 0; i < deliveries; i++ {
		candidate := event
		if i%2 == 1 {
			candidate.Quantity = 1000
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			bus := &recordingBus{}
			ctx := publish.WithPublisher(context.Background(), bus)
			err := consumer.OnCreated(ctx, candidate.Name(), candidate)
			results <- result{candidate.Quantity, bus.events, err}
		}()
	}
	wg.Wait()
	close(results)
	decision, exists := state.Decision(event.EventID)
	if !exists {
		t.Fatal("a decision must be stored")
	}
	for got := range results {
		if got.quantity != decision.Quantity {
			if got.err == nil || len(got.events) != 0 {
				t.Fatalf("conflicting delivery was accepted or published: %+v", got)
			}
			continue
		}
		if got.err != nil || len(got.events) != 1 {
			t.Fatalf("matching duplicate was not replayed: %+v", got)
		}
		switch result := got.events[0].(type) {
		case events.StockReserved:
			if decision.Outcome != "reserved" || result.Quantity != decision.Quantity || !result.At.Equal(decision.At) {
				t.Fatalf("result differs from stored decision: %+v, %+v", result, decision)
			}
		case events.StockRejected:
			if decision.Outcome != "rejected" || result.Quantity != decision.Quantity || !result.At.Equal(decision.At) {
				t.Fatalf("result differs from stored decision: %+v, %+v", result, decision)
			}
		default:
			t.Fatalf("unexpected result type %T", result)
		}
	}
}
