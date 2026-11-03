package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/internal/store"
	"github.com/NARUBROWN/spine/pkg/event/publish"
)

const availableQuantity = 100

type ReservationService struct {
	store *store.IdempotencyStore
}

func NewReservationService(idempotencyStore *store.IdempotencyStore) *ReservationService {
	return &ReservationService{store: idempotencyStore}
}

func (c *ReservationService) Reserve(ctx context.Context, eventName string, event events.OrderCreated) error {
	if err := validate(event); err != nil {
		return err
	}

	if event.Scenario == events.ScenarioPoison {
		log.Printf("Poison event rejected: event=%s event_id=%s order_id=%d", eventName, event.EventID, event.OrderID)
		return errors.New("poison event requested by demo scenario")
	}

	if decision, exists := c.store.Decision(event.EventID); exists {
		if err := sameOrder(event, decision); err != nil {
			return err
		}
		log.Printf("Duplicate event detected: event=%s event_id=%s order_id=%d outcome=%s", eventName, event.EventID, event.OrderID, decision.Outcome)
		publishDecision(ctx, decision)
		return nil
	}

	attempt, err := c.store.Attempt(event.EventID)
	if err != nil {
		return fmt.Errorf("record delivery attempt: %w", err)
	}
	if attempt <= event.FailuresBeforeSuccess {
		log.Printf("Transient processing failure: event=%s event_id=%s order_id=%d attempt=%d", eventName, event.EventID, event.OrderID, attempt)
		return fmt.Errorf("transient failure requested by demo scenario (attempt %d)", attempt)
	}

	decision, err := c.store.SaveOrLoad(decide(event))
	if err != nil {
		return fmt.Errorf("persist stock decision: %w", err)
	}
	if err := sameOrder(event, decision); err != nil {
		return err
	}
	publishDecision(ctx, decision)
	log.Printf("Stock decision completed: event=%s event_id=%s order_id=%d outcome=%s attempt=%d", eventName, event.EventID, event.OrderID, decision.Outcome, attempt)
	return nil
}

func sameOrder(event events.OrderCreated, decision store.Decision) error {
	if event.OrderID != decision.OrderID || event.Quantity != decision.Quantity {
		return fmt.Errorf("event_id %q conflicts with an existing stock decision", event.EventID)
	}
	return nil
}

func validate(event events.OrderCreated) error {
	if event.EventID == "" {
		return errors.New("event_id is required")
	}
	if event.OrderID <= 0 {
		return errors.New("order_id must be positive")
	}
	if event.Quantity <= 0 {
		return errors.New("quantity must be positive")
	}
	return nil
}

func decide(event events.OrderCreated) store.Decision {
	decision := store.Decision{
		EventID:  event.EventID,
		OrderID:  event.OrderID,
		Quantity: event.Quantity,
		Outcome:  "reserved",
		At:       time.Now().UTC(),
	}
	if event.Quantity > availableQuantity {
		decision.Outcome = "rejected"
		decision.Reason = "insufficient stock"
	}
	return decision
}

func publishDecision(ctx context.Context, decision store.Decision) {
	switch decision.Outcome {
	case "reserved":
		publish.Event(ctx, events.StockReserved{
			EventID:       "stock-reserved-" + decision.EventID,
			SourceEventID: decision.EventID,
			OrderID:       decision.OrderID,
			Quantity:      decision.Quantity,
			At:            decision.At,
		})
	case "rejected":
		publish.Event(ctx, events.StockRejected{
			EventID:       "stock-rejected-" + decision.EventID,
			SourceEventID: decision.EventID,
			OrderID:       decision.OrderID,
			Quantity:      decision.Quantity,
			Reason:        decision.Reason,
			At:            decision.At,
		})
	}
}
