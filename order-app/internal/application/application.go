package application

import (
	"os"
	"time"

	"github.com/NARUBROWN/spine"
	"github.com/NARUBROWN/spine-simple-msa/order-app/consumer"
	"github.com/NARUBROWN/spine-simple-msa/order-app/controller"
	"github.com/NARUBROWN/spine-simple-msa/order-app/state"
	"github.com/NARUBROWN/spine/pkg/boot"
)

// BuildApplication은 브로커나 HTTP 리스너에 연결하지 않고 주문 서비스를 구성합니다.
func BuildApplication() (spine.App, boot.Options, error) {
	app := spine.New()
	app.Constructor(state.NewOrderTracker, controller.NewOrderController, consumer.NewStockConsumer)
	app.Route("POST", "/orders/:order_id", (*controller.OrderController).Create)
	app.Route("POST", "/orders/:order_id/large", (*controller.OrderController).CreateLarge)
	app.Route("POST", "/scenarios/transient/:order_id", (*controller.OrderController).CreateTransient)
	app.Route("POST", "/scenarios/poison/:order_id", (*controller.OrderController).CreatePoison)
	app.Route("GET", "/orders/:order_id/status", (*controller.OrderController).Status)

	if err := app.Consumers().Register(os.Getenv("SPINE_DEMO_TOPIC_PREFIX")+"stock.reserved", (*consumer.StockConsumer).OnReserved); err != nil {
		return nil, boot.Options{}, err
	}
	if err := app.Consumers().Register(os.Getenv("SPINE_DEMO_TOPIC_PREFIX")+"stock.rejected", (*consumer.StockConsumer).OnRejected); err != nil {
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
