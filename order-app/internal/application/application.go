package application

import (
	"os"
	"time"

	stockconsumer "github.com/NARUBROWN/spine-simple-msa/order-app/src/consumer/stock"
	"github.com/NARUBROWN/spine-simple-msa/order-app/src/controller"
	"github.com/NARUBROWN/spine-simple-msa/order-app/src/route"
	"github.com/NARUBROWN/spine-simple-msa/order-app/src/service"

	"github.com/NARUBROWN/spine"
	echoplatform "github.com/NARUBROWN/spine-echo-platform"
	kafkaplatform "github.com/NARUBROWN/spine-kafka-platform"
	playground "github.com/NARUBROWN/spine-validator-playground"
	"github.com/NARUBROWN/spine/pkg/boot"
)

// BuildApplication은 브로커나 HTTP 리스너에 연결하지 않고 주문 서비스를 구성합니다.
func BuildApplication() (spine.App, boot.Options, error) {
	factory, err := kafkaplatform.NewFactory(kafkaplatform.Config{
		Brokers: []string{envOrDefault("SPINE_DEMO_KAFKA_BROKER", "localhost:9092")},
		Consume: true, Publish: true,
		GroupID: envOrDefault("SPINE_DEMO_ORDER_GROUP", "order-service"),
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
	app := spine.New(echoplatform.Option(),
		spine.WithEventAdapter("kafka", factory,
			spine.WithEventConsumerRetry(boot.ConsumerRetryOptions{
				InitialDelay: 200 * time.Millisecond, MaxDelay: 2 * time.Second, Multiplier: 2, Jitter: 0.2,
			}),
			spine.WithEventSettlementTimeout(5*time.Second),
		),
		spine.WithValidator(validator),
	)
	app.Constructor(service.NewOrderTrackerService, service.NewOrderService, controller.NewOrderController)
	if err := route.Register(app); err != nil {
		return nil, boot.Options{}, err
	}
	if err := stockconsumer.Register(app); err != nil {
		return nil, boot.Options{}, err
	}
	return app, boot.Options{
		Address:                envOrDefault("SPINE_DEMO_ORDER_ADDRESS", ":"+envOrDefault("SPINE_PORT", "8080")),
		EnableGracefulShutdown: true,
		ShutdownTimeout:        10 * time.Second,
		HTTP:                   &boot.HTTPOptions{MaxBodyBytes: 4096},
	}, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
