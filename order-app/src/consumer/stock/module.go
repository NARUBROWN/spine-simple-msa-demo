package stock

import (
	"os"

	"github.com/NARUBROWN/spine"
)

// Register는 재고 처리 결과의 생성자와 수신 계약을 등록합니다.
func Register(app spine.App) error {
	app.Constructor(NewStockConsumer)
	if err := app.Consumers().Register(os.Getenv("SPINE_DEMO_TOPIC_PREFIX")+"stock.reserved", (*StockConsumer).OnReserved); err != nil {
		return err
	}
	if err := app.Consumers().Register(os.Getenv("SPINE_DEMO_TOPIC_PREFIX")+"stock.rejected", (*StockConsumer).OnRejected); err != nil {
		return err
	}
	return nil
}
