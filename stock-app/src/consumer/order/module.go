package order

import "github.com/NARUBROWN/spine"

// Register는 주문 생성 토픽과 수신 메서드를 연결합니다.
func Register(app spine.App) error {
	return app.Consumers().Register("order.created", (*OrderConsumer).OnCreated)
}
