package controller

import (
	"context"
	"net/http"

	"github.com/NARUBROWN/spine-simple-msa/order-app/src/service"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/NARUBROWN/spine/pkg/httperr"
	"github.com/NARUBROWN/spine/pkg/httpx"
	"github.com/NARUBROWN/spine/pkg/path"
)

const (
	defaultQuantity = 1
	largeQuantity   = 1000
)

type OrderAccepted struct {
	EventID string `json:"event_id"`
	OrderID int64  `json:"order_id"`
	Status  string `json:"status"`
}

type OrderController struct {
	tracker *service.OrderTrackerService
}

func NewOrderController(tracker *service.OrderTrackerService) *OrderController {
	return &OrderController{tracker: tracker}
}

func (c *OrderController) Create(ctx context.Context, orderID path.Int) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.Value, defaultQuantity, events.ScenarioNormal, 0)
}

func (c *OrderController) CreateLarge(ctx context.Context, orderID path.Int) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.Value, largeQuantity, events.ScenarioNormal, 0)
}

func (c *OrderController) CreateTransient(ctx context.Context, orderID path.Int) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.Value, defaultQuantity, events.ScenarioTransient, 1)
}

func (c *OrderController) CreatePoison(ctx context.Context, orderID path.Int) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.Value, defaultQuantity, events.ScenarioPoison, 0)
}

func (c *OrderController) Status(_ context.Context, orderID path.Int) (httpx.Response[service.OrderStatus], error) {
	status, ok := c.tracker.Get(orderID.Value)
	if !ok {
		return httpx.Response[service.OrderStatus]{}, httperr.NotFound("order status not found")
	}
	return httpx.Response[service.OrderStatus]{Body: status}, nil
}

func (c *OrderController) publish(ctx context.Context, orderID int64, quantity int, scenario string, failuresBeforeSuccess int) (httpx.Response[OrderAccepted], error) {
	if orderID <= 0 {
		return httpx.Response[OrderAccepted]{}, httperr.BadRequest("order_id must be positive")
	}
	event := events.NewOrderCreated(orderID, quantity, scenario, failuresBeforeSuccess)
	status, accepted := c.tracker.Accepted(orderID, event.EventID)
	if !accepted {
		return httpx.Response[OrderAccepted]{}, &httperr.HTTPError{Status: http.StatusConflict, Message: "order_id already belongs to a different request"}
	}
	publish.Event(ctx, event)
	return httpx.Response[OrderAccepted]{
		Body:    OrderAccepted{EventID: event.EventID, OrderID: orderID, Status: status.Status},
		Options: httpx.ResponseOptions{Status: 202},
	}, nil
}
