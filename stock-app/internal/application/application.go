package application

import (
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	kafkaplatform "github.com/NARUBROWN/spine-kafka-platform"
	orderconsumer "github.com/NARUBROWN/spine-simple-msa/stock-app/src/consumer/order"
	service "github.com/NARUBROWN/spine-simple-msa/stock-app/src/service"
	playground "github.com/NARUBROWN/spine-validator-playground"
	"github.com/NARUBROWN/spine/pkg/boot"
	"github.com/NARUBROWN/spine/pkg/constructor"
)

// BuildApplication은 상태 파일을 읽거나 브로커에 연결하지 않고 재고 워커를 구성합니다.
func BuildApplication() (spine.App, boot.Options, error) {
	factory, err := kafkaplatform.NewFactory(kafkaplatform.Config{
		Brokers: []string{envOrDefault("SPINE_DEMO_KAFKA_BROKER", "localhost:9092")},
		Consume: true, Publish: true,
		GroupID: envOrDefault("SPINE_DEMO_STOCK_GROUP", "stock-service"),
		// 물리 토픽 접두사는 플랫폼에서만 적용하고 consumer는 논리 이름을 등록합니다.
		TopicPrefix:            os.Getenv("SPINE_DEMO_TOPIC_PREFIX"),
		AllowInsecureTransport: true, // 격리된 로컬 Kafka/Redpanda 예제입니다.
	})
	if err != nil {
		return nil, boot.Options{}, err
	}
	validator, err := playground.New()
	if err != nil {
		return nil, boot.Options{}, err
	}
	app := spine.New(
		spine.WithEventAdapter("kafka", factory,
			spine.WithEventConsumerRetry(boot.ConsumerRetryOptions{
				InitialDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second, Multiplier: 2, Jitter: 0.2,
			}),
			spine.WithEventSettlementTimeout(5*time.Second),
		),
		spine.WithValidator(validator),
	)
	app.Constructor(
		constructor.Custom(NewStockStore),
		constructor.Service(&service.ReservationService{}),
		constructor.Custom(&orderconsumer.OrderConsumer{}),
	)
	if err := orderconsumer.Register(app); err != nil {
		return nil, boot.Options{}, err
	}

	return app, boot.Options{
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
	}, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
