package redpanda

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Publisher publishes CloudEvents to Redpanda using a bounded lifecycle and batched acknowledged writes.
type Publisher struct {
	config          Config
	factory         ProducerFactory
	validator       TopicValidator
	mu              sync.Mutex
	opened          bool
	producers       map[string]Producer
	validatedTopics map[string]bool
}

// New constructs a Redpanda publisher with kafka-go based dependencies.
func New(config Config) *Publisher {
	return NewWithDependencies(config, KafkaProducerFactory{}, KafkaTopicValidator{})
}

// NewWithDependencies constructs a Redpanda publisher with injected dependencies for testing.
func NewWithDependencies(config Config, factory ProducerFactory, validator TopicValidator) *Publisher {
	if factory == nil {
		factory = KafkaProducerFactory{}
	}
	if validator == nil {
		validator = KafkaTopicValidator{}
	}
	return &Publisher{config: config.normalized(), factory: factory, validator: validator, producers: map[string]Producer{}, validatedTopics: map[string]bool{}}
}

// Name returns the adapter name.
func (p *Publisher) Name() string {
	return Name
}

// Open validates broker configuration and the configured static topic before events are processed.
func (p *Publisher) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(p.config.Brokers) == 0 {
		return fmt.Errorf("at least one Redpanda broker is required")
	}
	topics := []string{}
	if IsSingleTopicMode(p.config.TopicMode) {
		if p.config.Topic == "" {
			return fmt.Errorf("redpanda topic is required for single topic mode")
		}
		topics = append(topics, p.config.Topic)
	}
	if err := p.validator.ValidateTopics(ctx, p.config, topics); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.opened = true
	for _, topic := range topics {
		p.validatedTopics[topic] = true
	}
	return nil
}

// Publish writes one CloudEvent to Redpanda by delegating to the batch path.
func (p *Publisher) Publish(ctx context.Context, evt cloudevents.Event) error {
	return p.PublishBatch(ctx, []cloudevents.Event{evt})
}

// PublishBatch writes CloudEvents to Redpanda in topic-grouped batches and waits for broker acknowledgement.
func (p *Publisher) PublishBatch(ctx context.Context, events []cloudevents.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(events) == 0 {
		return nil
	}
	messagesByTopic := map[string][]Message{}
	for _, evt := range events {
		topic, message, err := p.messageForEvent(ctx, evt)
		if err != nil {
			return err
		}
		messagesByTopic[topic] = append(messagesByTopic[topic], message)
	}
	for topic, messages := range messagesByTopic {
		producer, err := p.producerForTopic(topic)
		if err != nil {
			return err
		}
		if err := producer.Write(ctx, messages...); err != nil {
			return fmt.Errorf("produce %d CloudEvents to Redpanda topic %s: %w", len(messages), topic, err)
		}
	}
	return nil
}

// Close releases all initialized Redpanda producers.
func (p *Publisher) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var firstErr error
	for topic, producer := range p.producers {
		if err := producer.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close Redpanda producer for topic %s: %w", topic, err)
		}
		delete(p.producers, topic)
	}
	p.opened = false
	return firstErr
}

// messageForEvent validates one CloudEvent, resolves its topic, and converts it to a Redpanda message.
func (p *Publisher) messageForEvent(ctx context.Context, evt cloudevents.Event) (string, Message, error) {
	if err := evt.Validate(); err != nil {
		return "", Message{}, fmt.Errorf("validate CloudEvent: %w", err)
	}
	topic, err := TopicFor(p.config, evt)
	if err != nil {
		return "", Message{}, err
	}
	if err := p.ensureTopic(ctx, topic); err != nil {
		return "", Message{}, err
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return "", Message{}, fmt.Errorf("marshal CloudEvent: %w", err)
	}
	message := Message{Topic: topic, Key: []byte(evt.ID()), Value: body, Headers: []Header{
		{Key: "content-type", Value: []byte("application/cloudevents+json")},
		{Key: "ce_id", Value: []byte(evt.ID())},
		{Key: "ce_type", Value: []byte(evt.Type())},
		{Key: "ce_source", Value: []byte(evt.Source())},
	}}
	return topic, message, nil
}

// ensureTopic validates dynamic topics once before creating a producer for them.
func (p *Publisher) ensureTopic(ctx context.Context, topic string) error {
	p.mu.Lock()
	opened := p.opened
	alreadyValidated := p.validatedTopics[topic]
	p.mu.Unlock()
	if !opened {
		return fmt.Errorf("redpanda publisher must be opened before publishing")
	}
	if alreadyValidated {
		return nil
	}
	if err := p.validator.ValidateTopics(ctx, p.config, []string{topic}); err != nil {
		return err
	}
	p.mu.Lock()
	p.validatedTopics[topic] = true
	p.mu.Unlock()
	return nil
}

// producerForTopic returns a lazily initialized synchronous producer for the target topic.
func (p *Publisher) producerForTopic(topic string) (Producer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if producer, ok := p.producers[topic]; ok {
		return producer, nil
	}
	producer, err := p.factory.NewProducer(p.config, topic)
	if err != nil {
		return nil, err
	}
	p.producers[topic] = producer
	return producer, nil
}
