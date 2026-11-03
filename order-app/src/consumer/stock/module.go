package stock

import (
	"github.com/NARUBROWN/spine"
)

// Register는 재고 처리 결과의 생성자와 수신 계약을 등록합니다.
func Register(app spine.App) error {
	app.Constructor(NewStockConsumer)
	if err := app.Consumers().Register("stock.reserved", (*StockConsumer).OnReserved); err != nil {
		return err
	}
	if err := app.Consumers().Register("stock.rejected", (*StockConsumer).OnRejected); err != nil {
		return err
	}
	return nil
}
