// Package redpanda exposes Redpanda Eventflow adapters.
package redpanda

import (
	"context"
	"io"

	eventflow "github.com/rezarajan/project-datascape"
	consumer "github.com/rezarajan/project-datascape/internal/adapters/consume/redpanda"
	producer "github.com/rezarajan/project-datascape/internal/adapters/fanout/redpanda"
)

// EmitterConfig configures a Redpanda emitter.
type EmitterConfig = producer.Config

// ReceiverConfig configures a Redpanda receiver.
type ReceiverConfig = consumer.Config

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
func (e *Emitter) EmitBatch(ctx context.Context, events []eventflow.Event) error {
	return e.inner.PublishBatch(ctx, events)
}

// Close closes the writer.
func (e *Emitter) Close(ctx context.Context) error { return e.inner.Close(ctx) }

// Receiver consumes CloudEvents from Redpanda.
type Receiver struct {
	inner  *consumer.Source
	buffer []eventflow.Event
}

// NewReceiver constructs a Redpanda receiver.
func NewReceiver(config ReceiverConfig) *Receiver {
	return &Receiver{inner: consumer.New(config)}
}

// Name returns the adapter name.
func (*Receiver) Name() string { return "redpanda" }

// Open opens the reader.
func (r *Receiver) Open(ctx context.Context) error { return r.inner.Open(ctx) }

// Receive reads one event, committing only after the underlying source accepts it.
func (r *Receiver) Receive(ctx context.Context) (eventflow.Event, error) {
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

// ReceiveBatch reads a bounded batch.
func (r *Receiver) ReceiveBatch(ctx context.Context, maxEvents int) ([]eventflow.Event, error) {
	return r.inner.ReadBatch(ctx, maxEvents)
}

// Close closes the reader.
func (r *Receiver) Close(ctx context.Context) error { return r.inner.Close(ctx) }
