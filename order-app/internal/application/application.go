package application

import (
	"os"
	"time"

	stockconsumer "github.com/NARUBROWN/spine-simple-msa/order-app/src/consumer/stock"
	"github.com/NARUBROWN/spine-simple-msa/order-app/src/order"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine/pkg/boot"
)

// BuildApplication은 브로커나 HTTP 리스너에 연결하지 않고 주문 서비스를 구성합니다.
func BuildApplication() (spine.App, boot.Options, error) {
	app := spine.New()
	if err := order.Register(app); err != nil {
		return nil, boot.Options{}, err
	}
	if err := stockconsumer.Register(app); err != nil {
		return nil, boot.Options{}, err
	}
	return app, boot.Options{
		Address:                envOrDefault("SPINE_DEMO_ORDER_ADDRESS", ":"+envOrDefault("SPINE_PORT", "8080")),
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		Kafka: &boot.KafkaOptions{
			Brokers:                []string{envOrDefault("SPINE_DEMO_KAFKA_BROKER", "localhost:9092")},
			AllowInsecureTransport: true,
			ConsumerRetry: boot.ConsumerRetryOptions{
				InitialDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second, Multiplier: 2, Jitter: 0.2,
			},
			Read:  &boot.KafkaReadOptions{GroupID: envOrDefault("SPINE_DEMO_ORDER_GROUP", "order-service")},
			Write: &boot.KafkaWriteOptions{TopicPrefix: os.Getenv("SPINE_DEMO_TOPIC_PREFIX")},
		},
		HTTP: &boot.HTTPOptions{},
	}, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
