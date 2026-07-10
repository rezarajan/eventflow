package redpanda

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/segmentio/kafka-go"

	"github.com/datascape/eventflow/internal/lineage"
)

// Source consumes CloudEvents from Redpanda.
type Source struct {
	config  Config
	factory ReaderFactory
	reader  Reader
}

// New constructs a Redpanda event source with kafka-go based dependencies.
func New(config Config) *Source {
	return NewWithDependencies(config, KafkaReaderFactory{})
}

// NewWithDependencies constructs a Redpanda source with injected dependencies for testing.
func NewWithDependencies(config Config, factory ReaderFactory) *Source {
	if factory == nil {
		factory = KafkaReaderFactory{}
	}
	return &Source{config: config.normalized(), factory: factory}
}

// Name returns the event source name.
func (s *Source) Name() string {
	return Name
}

// Dataset returns the stable Redpanda topic dataset consumed by this source.
func (s *Source) Dataset() lineage.Dataset {
	return lineage.RedpandaDataset(s.config.Brokers, s.config.Topic)
}

// Open validates configuration and initializes the underlying reader.
func (s *Source) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(s.config.Brokers) == 0 {
		return fmt.Errorf("at least one Redpanda broker is required")
	}
	if s.config.Topic == "" {
		return fmt.Errorf("redpanda topic is required")
	}
	reader, err := s.factory.NewReader(s.config)
	if err != nil {
		return err
	}
	s.reader = reader
	return nil
}

// ReadBatch reads up to maxEvents CloudEvents from Redpanda.
func (s *Source) ReadBatch(ctx context.Context, maxEvents int) ([]cloudevents.Event, error) {
	if s.reader == nil {
		return nil, fmt.Errorf("redpanda source must be opened before reading")
	}
	if maxEvents <= 0 {
		maxEvents = 100
	}
	events := make([]cloudevents.Event, 0, maxEvents)
	for len(events) < maxEvents {
		message, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if len(events) > 0 && (err == context.Canceled || err == context.DeadlineExceeded || err == io.EOF) {
				return events, nil
			}
			return events, err
		}
		evt, err := decodeCloudEvent(message.Value)
		if err != nil {
			return events, err
		}
		if err := s.reader.CommitMessages(ctx, message); err != nil {
			return events, fmt.Errorf("commit Redpanda message: %w", err)
		}
		events = append(events, evt)
	}
	return events, nil
}

// Close releases the Redpanda reader.
func (s *Source) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if s.reader == nil {
		return nil
	}
	err := s.reader.Close()
	s.reader = nil
	return err
}

// decodeCloudEvent decodes and validates one CloudEvent JSON payload.
func decodeCloudEvent(body []byte) (cloudevents.Event, error) {
	var evt cloudevents.Event
	if err := json.Unmarshal(body, &evt); err != nil {
		return cloudevents.Event{}, fmt.Errorf("decode CloudEvent from Redpanda message: %w", err)
	}
	if err := evt.Validate(); err != nil {
		return cloudevents.Event{}, fmt.Errorf("validate CloudEvent from Redpanda message: %w", err)
	}
	return evt, nil
}

// Reader reads and commits Redpanda messages.
type Reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

// ReaderFactory creates a Redpanda reader from configuration.
type ReaderFactory interface {
	NewReader(config Config) (Reader, error)
}

// KafkaReaderFactory creates kafka-go readers for Redpanda topics.
type KafkaReaderFactory struct{}

// NewReader constructs a kafka-go reader for Redpanda.
func (f KafkaReaderFactory) NewReader(config Config) (Reader, error) {
	config = config.normalized()
	if len(config.Brokers) == 0 {
		return nil, fmt.Errorf("at least one Redpanda broker is required")
	}
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     config.Brokers,
		Topic:       config.Topic,
		GroupID:     config.GroupID,
		StartOffset: kafkaStartOffset(config.StartOffset),
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     100 * time.Millisecond,
	}), nil
}

// kafkaStartOffset converts a string setting into a kafka-go start offset.
func kafkaStartOffset(value string) int64 {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "last", "latest", "end":
		return kafka.LastOffset
	default:
		return kafka.FirstOffset
	}
}
