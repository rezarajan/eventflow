// Package fanout defines output publisher ports for CloudEvents fan-out.
package fanout

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Publisher publishes CloudEvents SDK events to an output target through an explicit lifecycle.
type Publisher interface {
	Name() string
	Open(ctx context.Context) error
	Publish(ctx context.Context, event cloudevents.Event) error
	Close(ctx context.Context) error
}

// BatchPublisher publishes groups of CloudEvents when an adapter can use transport-level batching.
type BatchPublisher interface {
	Publisher
	PublishBatch(ctx context.Context, events []cloudevents.Event) error
}

// Factory creates named publishers from configuration.
type Factory interface {
	Names() []string
	Create(name string) (Publisher, error)
}
