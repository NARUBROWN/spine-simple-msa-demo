package dto

type CreateOrderRequest struct {
	OrderID  int64 `json:"order_id" validate:"gt=0"`
	Quantity int   `json:"quantity" validate:"gt=0"`
}
