package redpanda

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// TestPublisherOpenValidatesSingleTopic verifies preflight validation happens before event processing.
func TestPublisherOpenValidatesSingleTopic(t *testing.T) {
	validator := &fakeTopicValidator{}
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "example.events.v1", TopicMode: "single"}, &fakeProducerFactory{}, validator)
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if got := validator.calls[0]; got != "example.events.v1" {
		t.Fatalf("validated topic = %q, want example.events.v1", got)
	}
}

// TestPublisherOpenRequiresBrokers verifies broker configuration is mandatory.
func TestPublisherOpenRequiresBrokers(t *testing.T) {
	publisher := NewWithDependencies(Config{Topic: "example.events.v1", TopicMode: "single"}, &fakeProducerFactory{}, &fakeTopicValidator{})
	err := publisher.Open(context.Background())
	if err == nil || !strings.Contains(err.Error(), "broker") {
		t.Fatalf("expected broker error, got %v", err)
	}
}

// TestPublisherRequiresOpenBeforePublish verifies fan-out must use the explicit lifecycle.
func TestPublisherRequiresOpenBeforePublish(t *testing.T) {
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "example.events.v1", TopicMode: "single"}, &fakeProducerFactory{}, &fakeTopicValidator{})
	err := publisher.Publish(context.Background(), redpandaTestEvent(t, "1", "thing.created.v1"))
	if err == nil || !strings.Contains(err.Error(), "must be opened") {
		t.Fatalf("expected open lifecycle error, got %v", err)
	}
}

// TestPublisherPublishWritesSynchronousMessage verifies CloudEvents are encoded and sent through an injected producer.
func TestPublisherPublishWritesSynchronousMessage(t *testing.T) {
	factory := &fakeProducerFactory{}
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "example.events.v1", TopicMode: "single"}, factory, &fakeTopicValidator{})
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), redpandaTestEvent(t, "1", "thing.created.v1")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	producer := factory.producers["example.events.v1"]
	if producer == nil || len(producer.messages) != 1 {
		t.Fatalf("expected one produced message, got %+v", producer)
	}
	message := producer.messages[0]
	if string(message.Key) != "1" || message.Topic != "example.events.v1" {
		t.Fatalf("unexpected message routing: topic=%s key=%s", message.Topic, message.Key)
	}
	if !hasHeader(message.Headers, "ce_type", "thing.created.v1") || !hasHeader(message.Headers, "content-type", "application/cloudevents+json") {
		t.Fatalf("missing CloudEvents headers: %+v", message.Headers)
	}
}

// TestPublisherTypePrefixValidatesDynamicTopic verifies dynamic topic mode validates each new topic once.
func TestPublisherTypePrefixValidatesDynamicTopic(t *testing.T) {
	validator := &fakeTopicValidator{}
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, TopicMode: "type-prefix"}, &fakeProducerFactory{}, validator)
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), redpandaTestEvent(t, "1", "example.created.v1")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), redpandaTestEvent(t, "2", "example.created.v1")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if len(validator.calls) != 1 || validator.calls[0] != "example.events.v1" {
		t.Fatalf("validator calls = %v, want one attendance topic validation", validator.calls)
	}
}

// TestPublisherCloseClosesProducers verifies Close releases created producers.
func TestPublisherCloseClosesProducers(t *testing.T) {
	factory := &fakeProducerFactory{}
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "example.events.v1", TopicMode: "single"}, factory, &fakeTopicValidator{})
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if err := publisher.Publish(context.Background(), redpandaTestEvent(t, "1", "thing.created.v1")); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
	if err := publisher.Close(context.Background()); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if !factory.producers["example.events.v1"].closed {
		t.Fatal("expected producer to be closed")
	}
}

// TestPublisherPublishBatchWritesOneProducerCall verifies batch publishing writes multiple events in one producer call.
func TestPublisherPublishBatchWritesOneProducerCall(t *testing.T) {
	factory := &fakeProducerFactory{}
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "example.events.v1", TopicMode: "single"}, factory, &fakeTopicValidator{})
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	events := []cloudevents.Event{redpandaTestEvent(t, "1", "thing.created.v1"), redpandaTestEvent(t, "2", "thing.created.v1")}
	if err := publisher.PublishBatch(context.Background(), events); err != nil {
		t.Fatalf("PublishBatch returned error: %v", err)
	}
	producer := factory.producers["example.events.v1"]
	if producer.writeCalls != 1 || len(producer.messages) != 2 {
		t.Fatalf("expected one producer call with two messages; calls=%d messages=%d", producer.writeCalls, len(producer.messages))
	}
}

// TestPublisherPropagatesProducerErrors verifies write errors surface with topic context.
func TestPublisherPropagatesProducerErrors(t *testing.T) {
	factory := &fakeProducerFactory{writeErr: fmt.Errorf("write failed")}
	publisher := NewWithDependencies(Config{Brokers: []string{"broker:9092"}, Topic: "example.events.v1", TopicMode: "single"}, factory, &fakeTopicValidator{})
	if err := publisher.Open(context.Background()); err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	err := publisher.Publish(context.Background(), redpandaTestEvent(t, "1", "thing.created.v1"))
	if err == nil || !strings.Contains(err.Error(), "produce") {
		t.Fatalf("expected produce error, got %v", err)
	}
}

// redpandaTestEvent constructs a valid CloudEvent for Redpanda publisher tests.
func redpandaTestEvent(t *testing.T, id string, eventType string) cloudevents.Event {
	t.Helper()
	evt := cloudevents.NewEvent(cloudevents.VersionV1)
	evt.SetID(id)
	evt.SetSource("urn:test")
	evt.SetType(eventType)
	evt.SetTime(time.Date(2026, 7, 9, 1, 0, 0, 0, time.UTC))
	if err := evt.SetData(cloudevents.ApplicationJSON, map[string]any{"id": id}); err != nil {
		t.Fatalf("set data: %v", err)
	}
	if err := evt.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}
	return evt
}

// hasHeader reports whether a message contains a header with the expected value.
func hasHeader(headers []Header, key string, value string) bool {
	for _, header := range headers {
		if header.Key == key && string(header.Value) == value {
			return true
		}
	}
	return false
}

// fakeTopicValidator records topic validation requests without connecting to Redpanda.
type fakeTopicValidator struct {
	calls []string
	err   error
}

// ValidateTopics records topics and returns the configured error.
func (v *fakeTopicValidator) ValidateTopics(ctx context.Context, config Config, topics []string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	v.calls = append(v.calls, topics...)
	return v.err
}

// fakeProducerFactory creates in-memory producers keyed by topic.
type fakeProducerFactory struct {
	producers map[string]*fakeProducer
	writeErr  error
	closeErr  error
}

// NewProducer creates or records an in-memory producer for a topic.
func (f *fakeProducerFactory) NewProducer(config Config, topic string) (Producer, error) {
	if f.producers == nil {
		f.producers = map[string]*fakeProducer{}
	}
	producer := &fakeProducer{writeErr: f.writeErr, closeErr: f.closeErr}
	f.producers[topic] = producer
	return producer, nil
}

// fakeProducer records produced messages in memory.
type fakeProducer struct {
	messages   []Message
	writeCalls int
	closed     bool
	writeErr   error
	closeErr   error
}

// Write records messages or returns the configured write error.
func (p *fakeProducer) Write(ctx context.Context, messages ...Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.writeErr != nil {
		return p.writeErr
	}
	p.writeCalls++
	p.messages = append(p.messages, messages...)
	return nil
}

// Close records closure or returns the configured close error.
func (p *fakeProducer) Close() error {
	if p.closeErr != nil {
		return p.closeErr
	}
	p.closed = true
	return nil
}
