package order

import "github.com/NARUBROWN/spine"

// Register는 주문 상태와 HTTP 라우트를 등록합니다.
func Register(app spine.App) error {
	app.Constructor(NewOrderTracker, NewOrderController)
	app.Route("POST", "/orders/:order_id", (*OrderController).Create)
	app.Route("POST", "/orders/:order_id/large", (*OrderController).CreateLarge)
	app.Route("POST", "/scenarios/transient/:order_id", (*OrderController).CreateTransient)
	app.Route("POST", "/scenarios/poison/:order_id", (*OrderController).CreatePoison)
	app.Route("GET", "/orders/:order_id/status", (*OrderController).Status)
	return nil
}
