package controller

import (
	"context"
	"time"

	"github.com/NARUBROWN/spine-simple-msa/shared/events"
	"github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/NARUBROWN/spine/pkg/httpx"
	"github.com/NARUBROWN/spine/pkg/path"
)

type OrderController struct{}

func NewOrderController() *OrderController {
	return &OrderController{}
}

func (c *OrderController) Create(ctx context.Context, orderId path.Int) httpx.Response[string] {
	publish.Event(ctx, &events.OrderCreated{
		OrderID: orderId.Value,
		At:      time.Now(),
	})

	return httpx.Response[string]{Body: "OK"}
}
