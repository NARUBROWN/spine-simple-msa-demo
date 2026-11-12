package stock

import (
	"github.com/NARUBROWN/spine"
)

// Register는 재고 결과 토픽과 수신 메서드를 연결합니다.
func Register(app spine.App) error {
	if err := app.Consumers().Register("stock.reserved", (*StockConsumer).OnReserved); err != nil {
		return err
	}
	return app.Consumers().Register("stock.rejected", (*StockConsumer).OnRejected)
}
