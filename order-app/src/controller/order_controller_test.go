package controller

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/NARUBROWN/spine-simple-msa/order-app/src/service"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/NARUBROWN/spine/pkg/httperr"
)

type recordingBus struct {
	events []publish.DomainEvent
}

func (b *recordingBus) Publish(events ...publish.DomainEvent) {
	b.events = append(b.events, events...)
}

func TestChangedQuantityReturnsConflictWithoutPublishing(t *testing.T) {
	bus := &recordingBus{}
	ctx := publish.WithPublisher(context.Background(), bus)
	controller := (&OrderController{}).Constructor((&service.OrderService{}).Constructor((&service.OrderTrackerService{}).Constructor()))
	const orderID int64 = 101
	if _, err := controller.publish(ctx, orderID, defaultQuantity, events.ScenarioNormal, 0); err != nil {
		t.Fatal(err)
	}
	if len(bus.events) != 1 {
		t.Fatalf("initial request published %d events", len(bus.events))
	}
	_, err := controller.publish(ctx, orderID, largeQuantity, events.ScenarioNormal, 0)
	var httpError *httperr.HTTPError
	if !errors.As(err, &httpError) || httpError.Status != http.StatusConflict {
		t.Fatalf("conflicting request error = %v, want HTTP 409", err)
	}
	if len(bus.events) != 1 {
		t.Fatalf("conflicting request published another event: %d", len(bus.events))
	}
}
