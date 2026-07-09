package redpanda

import "context"

// Header represents one message header published to Redpanda.
type Header struct {
	Key   string
	Value []byte
}

// Message represents a transport-neutral message before the Kafka adapter converts it.
type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []Header
}

// Producer writes messages to one Redpanda topic and releases resources when closed.
type Producer interface {
	Write(ctx context.Context, messages ...Message) error
	Close() error
}

// ProducerFactory creates a producer for a specific Redpanda topic.
type ProducerFactory interface {
	NewProducer(config Config, topic string) (Producer, error)
}

// TopicValidator verifies broker reachability and topic existence without creating topics.
type TopicValidator interface {
	ValidateTopics(ctx context.Context, config Config, topics []string) error
}
