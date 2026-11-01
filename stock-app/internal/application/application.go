package application

import (
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	orderconsumer "github.com/NARUBROWN/spine-simple-msa/stock-app/src/consumer/order"
	"github.com/NARUBROWN/spine/pkg/boot"
)

// BuildApplication은 상태 파일을 읽거나 브로커에 연결하지 않고 재고 워커를 구성합니다.
func BuildApplication() (spine.App, boot.Options, error) {
	app := spine.New()
	if err := orderconsumer.Register(app); err != nil {
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

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
