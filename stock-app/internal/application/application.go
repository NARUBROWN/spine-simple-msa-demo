package application

import (
	"fmt"
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/consumer"
	"github.com/NARUBROWN/spine-simple-msa/stock-app/store"
	"github.com/NARUBROWN/spine/pkg/boot"
)

// BuildApplication은 상태 파일을 읽거나 브로커에 연결하지 않고 재고 워커를 구성합니다.
func BuildApplication() (spine.App, boot.Options, error) {
	app := spine.New()
	app.Constructor(NewStockStore, consumer.NewOrderConsumer)
	if err := app.Consumers().Register(os.Getenv("SPINE_DEMO_TOPIC_PREFIX")+"order.created", (*consumer.OrderConsumer).OnCreated); err != nil {
		return nil, boot.Options{}, err
	}
	return app, boot.Options{
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers:                []string{envOrDefault("SPINE_DEMO_KAFKA_BROKER", "localhost:9092")},
			AllowInsecureTransport: true,
			ConsumerRetry: boot.ConsumerRetryOptions{
				InitialDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second, Multiplier: 2, Jitter: 0.2,
			},
			Read:  &boot.KafkaReadOptions{GroupID: envOrDefault("SPINE_DEMO_STOCK_GROUP", "stock-service")},
			Write: &boot.KafkaWriteOptions{TopicPrefix: os.Getenv("SPINE_DEMO_TOPIC_PREFIX")},
		},
	}, nil
}

// NewStockStore는 실제 워커가 시작할 때만 상태 파일을 여는 지연 생성자입니다.
func NewStockStore() *store.IdempotencyStore {
	state, err := store.NewIdempotencyStore(envOrDefault("SPINE_DEMO_STOCK_STATE", ".spine-demo/stock-state.json"))
	if err != nil {
		panic(fmt.Errorf("initialize idempotency store: %w", err))
	}
	return state
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
