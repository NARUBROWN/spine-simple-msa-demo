package outbox

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type KafkaPublisher struct {
	writer      *kafka.Writer
	topicPrefix string
}

func NewKafkaPublisher(broker, topicPrefix string) *KafkaPublisher {
	return &KafkaPublisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			RequiredAcks: kafka.RequireAll,
		},
		topicPrefix: topicPrefix,
	}
}

func (p *KafkaPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{Topic: p.topicPrefix + topic, Value: payload})
}

func (p *KafkaPublisher) Close() error {
	return p.writer.Close()
}
