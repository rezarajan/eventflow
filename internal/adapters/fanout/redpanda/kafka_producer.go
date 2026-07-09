package redpanda

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducerFactory creates kafka-go producers for Redpanda topics.
type KafkaProducerFactory struct{}

// NewProducer constructs a batched synchronous kafka-go writer for one Redpanda topic.
func (f KafkaProducerFactory) NewProducer(config Config, topic string) (Producer, error) {
	config = config.normalized()
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one Redpanda broker is required")
	}
	return &KafkaProducer{writer: &kafka.Writer{
		Addr:                   kafka.TCP(config.Brokers...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		BatchSize:              config.BatchSize,
		BatchTimeout:           time.Millisecond,
		MaxAttempts:            1,
		AllowAutoTopicCreation: false,
		Async:                  false,
	}}, nil
}

// KafkaProducer adapts kafka-go Writer to the Redpanda producer interface.
type KafkaProducer struct {
	writer *kafka.Writer
}

// Write synchronously writes a batch of messages and returns after broker acknowledgement or context cancellation.
func (p *KafkaProducer) Write(ctx context.Context, messages ...Message) error {
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for _, message := range messages {
		kafkaMessages = append(kafkaMessages, kafkaMessageForTopicWriter(message))
	}
	return p.writer.WriteMessages(ctx, kafkaMessages...)
}

// kafkaMessageForTopicWriter converts a message for a writer already bound to one topic.
func kafkaMessageForTopicWriter(message Message) kafka.Message {
	headers := make([]kafka.Header, 0, len(message.Headers))
	for _, header := range message.Headers {
		headers = append(headers, kafka.Header{Key: header.Key, Value: header.Value})
	}
	return kafka.Message{Key: message.Key, Value: message.Value, Headers: headers}
}

// Close releases the underlying kafka-go writer.
func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

// KafkaTopicValidator validates configured Redpanda topics by inspecting broker metadata.
type KafkaTopicValidator struct{}

// ValidateTopics verifies that all required topics exist before events are processed.
func (v KafkaTopicValidator) ValidateTopics(ctx context.Context, config Config, topics []string) error {
	if len(config.Brokers) == 0 {
		return fmt.Errorf("at least one Redpanda broker is required")
	}
	if len(topics) == 0 {
		return nil
	}
	conn, err := kafka.DialContext(ctx, "tcp", config.Brokers[0])
	if err != nil {
		return fmt.Errorf("connect to Redpanda broker %s: %w", config.Brokers[0], err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("read Redpanda topic metadata: %w", err)
	}
	seen := map[string]bool{}
	for _, partition := range partitions {
		seen[partition.Topic] = true
	}
	for _, topic := range topics {
		if !seen[topic] {
			return fmt.Errorf("redpanda topic %q does not exist; create it before running fanout", topic)
		}
	}
	return nil
}
