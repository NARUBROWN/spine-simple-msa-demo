package application

import (
	"fmt"
	"os"

	"github.com/NARUBROWN/spine-simple-msa/stock-app/internal/store"
)

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
