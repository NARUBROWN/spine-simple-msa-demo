package route

import (
	"github.com/NARUBROWN/spine"
	controller "github.com/NARUBROWN/spine-simple-msa/order-app/src/controller"
)

// Register는 HTTP 라우트를 등록합니다.
func Register(app spine.App) error {
	app.Route("POST", "/orders", (*controller.OrderController).Submit)
	app.Route("GET", "/orders/:order_id/status", (*controller.OrderController).Status)
	app.Route("POST", "/scenarios/poison/:order_id", (*controller.OrderController).CreatePoison)
	app.Route("POST", "/scenarios/transient/:order_id", (*controller.OrderController).CreateTransient)
	app.Route("POST", "/orders/:order_id/large", (*controller.OrderController).CreateLarge)
	app.Route("POST", "/orders/:order_id", (*controller.OrderController).Create)
	return nil
}
