package controller

import (
	"context"
	"errors"
	"net/http"

	"github.com/NARUBROWN/spine-simple-msa/order-app/src/dto"
	"github.com/NARUBROWN/spine-simple-msa/order-app/src/service"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/httperr"
	"github.com/NARUBROWN/spine/pkg/httpx"
	"github.com/NARUBROWN/spine/pkg/input"
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
	orders *service.OrderService
}

func NewOrderController(orders *service.OrderService) *OrderController {
	return &OrderController{orders: orders}
}

func (c *OrderController) Create(ctx context.Context, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.OrderID, defaultQuantity, events.ScenarioNormal, 0)
}

func (c *OrderController) CreateLarge(ctx context.Context, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.OrderID, largeQuantity, events.ScenarioNormal, 0)
}

func (c *OrderController) CreateTransient(ctx context.Context, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.OrderID, defaultQuantity, events.ScenarioTransient, 1)
}

func (c *OrderController) CreatePoison(ctx context.Context, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, orderID.OrderID, defaultQuantity, events.ScenarioPoison, 0)
}

func (c *OrderController) Status(_ context.Context, orderID input.Path[dto.OrderPath]) (httpx.Response[service.OrderStatus], error) {
	status, ok := c.orders.Status(orderID.OrderID)
	if !ok {
		return httpx.Response[service.OrderStatus]{}, httperr.NotFound("order status not found")
	}
	return httpx.Response[service.OrderStatus]{Body: status}, nil
}

func (c *OrderController) publish(ctx context.Context, orderID int64, quantity int, scenario string, failuresBeforeSuccess int) (httpx.Response[OrderAccepted], error) {
	status, err := c.orders.Place(ctx, orderID, quantity, scenario, failuresBeforeSuccess)
	if errors.Is(err, service.ErrInvalidOrder) {
		return httpx.Response[OrderAccepted]{}, httperr.BadRequest(err.Error())
	}
	if errors.Is(err, service.ErrOrderConflict) {
		return httpx.Response[OrderAccepted]{}, &httperr.HTTPError{Status: http.StatusConflict, Message: err.Error()}
	}
	if err != nil {
		return httpx.Response[OrderAccepted]{}, err
	}
	return httpx.Response[OrderAccepted]{
		Body:    OrderAccepted{EventID: status.EventID, OrderID: orderID, Status: status.Status},
		Options: httpx.ResponseOptions{Status: http.StatusAccepted},
	}, nil
}

// Submit의 JSON 바인딩과 validate 태그 검사는 Spine이 핸들러 호출 전에 수행합니다.
func (c *OrderController) Submit(ctx context.Context, request input.Body[dto.CreateOrderRequest]) (httpx.Response[OrderAccepted], error) {
	return c.publish(ctx, request.OrderID, request.Quantity, events.ScenarioNormal, 0)
}
