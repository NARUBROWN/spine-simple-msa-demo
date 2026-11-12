package controller

import (
	"context"
	"errors"
	"net/http"

	"github.com/NARUBROWN/spine-simple-msa/order-app/src/dto"
	"github.com/NARUBROWN/spine-simple-msa/order-app/src/service"
	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/api"
	"github.com/NARUBROWN/spine/pkg/httperr"
	"github.com/NARUBROWN/spine/pkg/httpx"
	"github.com/NARUBROWN/spine/pkg/input"
	spinectx "github.com/NARUBROWN/spine/pkg/spine"
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

func (*OrderController) Constructor(orders *service.OrderService) *OrderController {
	return &OrderController{orders: orders}
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

// RegisterRoutes는 경로와 요청 처리 콜백을 함께 선언합니다.
func (c *OrderController) RegisterRoutes(r *api.Registry) {
	orders := r.Group("/orders", api.Tags("Orders"))
	orders.POST("/", func(ctx spinectx.Ctx, request input.Body[dto.CreateOrderRequest]) (httpx.Response[OrderAccepted], error) {
		return c.publish(ctx.ExecutionContext().Context(), request.OrderID, request.Quantity, events.ScenarioNormal, 0)
	})
	orders.GET("/:order_id/status", func(_ spinectx.Ctx, orderID input.Path[dto.OrderPath]) (httpx.Response[service.OrderStatus], error) {
		status, ok := c.orders.Status(orderID.OrderID)
		if !ok {
			return httpx.Response[service.OrderStatus]{}, httperr.NotFound("order status not found")
		}
		return httpx.Response[service.OrderStatus]{Body: status}, nil
	})
	orders.POST("/:order_id/large", func(ctx spinectx.Ctx, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
		return c.publish(ctx.ExecutionContext().Context(), orderID.OrderID, largeQuantity, events.ScenarioNormal, 0)
	})
	orders.POST("/:order_id", func(ctx spinectx.Ctx, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
		return c.publish(ctx.ExecutionContext().Context(), orderID.OrderID, defaultQuantity, events.ScenarioNormal, 0)
	})
	scenarios := r.Group("/scenarios", api.Tags("Scenarios"))
	scenarios.POST("/poison/:order_id", func(ctx spinectx.Ctx, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
		return c.publish(ctx.ExecutionContext().Context(), orderID.OrderID, defaultQuantity, events.ScenarioPoison, 0)
	})
	scenarios.POST("/transient/:order_id", func(ctx spinectx.Ctx, orderID input.Path[dto.OrderPath]) (httpx.Response[OrderAccepted], error) {
		return c.publish(ctx.ExecutionContext().Context(), orderID.OrderID, defaultQuantity, events.ScenarioTransient, 1)
	})
}
