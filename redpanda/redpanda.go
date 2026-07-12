// Package redpanda implements OpenLineage admission and quarantine gateway Redpanda transport.
package redpanda

import (
	"context"
	"fmt"
	"io"

	eventflow "github.com/rezarajan/eventflow"
	consumer "github.com/rezarajan/eventflow/internal/adapters/consume/redpanda"
	producer "github.com/rezarajan/eventflow/internal/adapters/fanout/redpanda"
	"github.com/rezarajan/eventflow/resource"
)

// EmitterConfig configures a Redpanda emitter.
type EmitterConfig = producer.Config

// ReceiverConfig configures a Redpanda receiver.
type ReceiverConfig = consumer.Config

// EmitterSpec is the declarative spec for RedpandaEmitter.
type EmitterSpec struct {
	Brokers   []string `yaml:"brokers" json:"brokers"`
	Topic     string   `yaml:"topic" json:"topic"`
	TopicMode string   `yaml:"topicMode,omitempty" json:"topicMode,omitempty"`
	BatchSize int      `yaml:"batchSize,omitempty" json:"batchSize,omitempty"`
}

// ReceiverSpec is the declarative spec for RedpandaReceiver.
type ReceiverSpec struct {
	Brokers     []string `yaml:"brokers" json:"brokers"`
	Topic       string   `yaml:"topic" json:"topic"`
	GroupID     string   `yaml:"groupId,omitempty" json:"groupId,omitempty"`
	StartOffset string   `yaml:"startOffset,omitempty" json:"startOffset,omitempty"`
}

// Register adds RedpandaEmitter and RedpandaReceiver resource definitions.
func Register(catalog *resource.Catalog) error {
	if err := resource.Register(catalog, resource.Definition[EmitterSpec]{
		GVK: resource.GVK("RedpandaEmitter"),
		Default: func(spec *EmitterSpec) error {
			if spec.TopicMode == "" {
				spec.TopicMode = "single"
			}
			if spec.BatchSize <= 0 {
				spec.BatchSize = 100
			}
			return nil
		},
		Validate: func(_ context.Context, spec EmitterSpec) error {
			if len(spec.Brokers) == 0 {
				return fmt.Errorf("brokers is required")
			}
			if spec.Topic == "" {
				return fmt.Errorf("topic is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec EmitterSpec) (any, error) {
			return NewEmitter(EmitterConfig{Brokers: spec.Brokers, Topic: spec.Topic, TopicMode: spec.TopicMode, BatchSize: spec.BatchSize}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityRedpandaEmitter},
	}); err != nil {
		return err
	}
	return resource.Register(catalog, resource.Definition[ReceiverSpec]{
		GVK: resource.GVK("RedpandaReceiver"),
		Default: func(spec *ReceiverSpec) error {
			if spec.GroupID == "" {
				spec.GroupID = "eventflow"
			}
			if spec.StartOffset == "" {
				spec.StartOffset = "first"
			}
			return nil
		},
		Validate: func(_ context.Context, spec ReceiverSpec) error {
			if len(spec.Brokers) == 0 {
				return fmt.Errorf("brokers is required")
			}
			if spec.Topic == "" {
				return fmt.Errorf("topic is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec ReceiverSpec) (any, error) {
			return NewReceiver(ReceiverConfig{Brokers: spec.Brokers, Topic: spec.Topic, GroupID: spec.GroupID, StartOffset: spec.StartOffset}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityRedpandaReceiver},
	})
}

// Emitter publishes CloudEvents to Redpanda.
type Emitter struct{ inner *producer.Publisher }

// NewEmitter constructs a Redpanda emitter.
func NewEmitter(config EmitterConfig) *Emitter {
	return &Emitter{inner: producer.New(config)}
}

// Name returns the adapter name.
func (*Emitter) Name() string { return "redpanda" }

// Open opens the Redpanda writer.
func (e *Emitter) Open(ctx context.Context) error { return e.inner.Open(ctx) }

// Emit writes one event.
func (e *Emitter) Emit(ctx context.Context, event eventflow.Event) error {
	return e.inner.Publish(ctx, event)
}

// EmitBatch writes a batch of events.
// Close closes the writer.
func (e *Emitter) Close(ctx context.Context) error { return e.inner.Close(ctx) }

// Receiver consumes CloudEvents from Redpanda.
type Receiver struct {
	inner     *consumer.Source
	principal string
	buffer    []eventflow.Event
	ackBuffer []eventflow.ReceivedEvent
}

// NewReceiver constructs a Redpanda receiver.
func NewReceiver(config ReceiverConfig) *Receiver {
	principal := "kafka://" + config.GroupID
	if config.GroupID == "" {
		principal = "kafka://eventflow"
	}
	return &Receiver{inner: consumer.New(config), principal: principal}
}

// Name returns the adapter name.
func (*Receiver) Name() string { return "redpanda" }

// Open opens the reader.
func (r *Receiver) Open(ctx context.Context) error { return r.inner.Open(ctx) }

// Receive reads one event, committing only after the underlying source accepts it.
func (r *Receiver) Receive(ctx context.Context) (eventflow.ReceivedEvent, error) {
	return r.ReceiveAck(ctx)
}

func (r *Receiver) receiveEvent(ctx context.Context) (eventflow.Event, error) {
	if len(r.buffer) == 0 {
		events, err := r.ReceiveBatch(ctx, 1)
		if err != nil {
			return eventflow.Event{}, err
		}
		if len(events) == 0 {
			return eventflow.Event{}, io.EOF
		}
		r.buffer = events
	}
	event := r.buffer[0]
	r.buffer = r.buffer[1:]
	return event, nil
}

// ReceiveAck reads one event and exposes an acknowledgement callback for offset commit.
func (r *Receiver) ReceiveAck(ctx context.Context) (eventflow.ReceivedEvent, error) {
	if len(r.ackBuffer) == 0 {
		events, err := r.inner.ReadBatchAck(ctx, 1)
		if err != nil {
			return eventflow.ReceivedEvent{}, err
		}
		if len(events) == 0 {
			return eventflow.ReceivedEvent{}, io.EOF
		}
		for _, event := range events {
			commit := event.Commit
			r.ackBuffer = append(r.ackBuffer, eventflow.ReceivedEvent{
				Event:     event.Event,
				Principal: r.principal,
				Ack:       commit,
				Nack:      func(context.Context) error { return nil },
			})
		}
	}
	event := r.ackBuffer[0]
	r.ackBuffer = r.ackBuffer[1:]
	return event, nil
}

// ReceiveBatch reads a bounded batch.
func (r *Receiver) ReceiveBatch(ctx context.Context, maxEvents int) ([]eventflow.Event, error) {
	return r.inner.ReadBatch(ctx, maxEvents)
}

// Close closes the reader.
func (r *Receiver) Close(ctx context.Context) error { return r.inner.Close(ctx) }
