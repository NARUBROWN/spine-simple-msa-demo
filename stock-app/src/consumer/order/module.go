package order

import (
	"fmt"
	"os"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/internal/store"
)

// Register는 지연 저장소 생성자와 주문 이벤트 수신 계약을 등록합니다.
func Register(app spine.App) error {
	app.Constructor(NewStockStore, NewOrderConsumer)
	if err := app.Consumers().Register(os.Getenv("SPINE_DEMO_TOPIC_PREFIX")+"order.created", (*OrderConsumer).OnCreated); err != nil {
		return err
	}
	return nil
}

// NewStockStore는 실제 워커가 시작할 때만 상태 파일을 여는 지연 생성자입니다.
func NewStockStore() *store.IdempotencyStore {
	state, err := store.NewIdempotencyStore(stockStatePath())
	if err != nil {
		panic(fmt.Errorf("initialize idempotency store: %w", err))
	}
	return state
}

func stockStatePath() string {
	if value := os.Getenv("SPINE_DEMO_STOCK_STATE"); value != "" {
		return value
	}
	return ".spine-demo/stock-state.json"
}
