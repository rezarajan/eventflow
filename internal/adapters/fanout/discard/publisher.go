// Package discard provides a publisher that accepts CloudEvents and intentionally drops them.
package discard

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Name is the adapter name used by fan-out configuration.
const Name = "discard"

// Publisher drops all events after accepting them.
type Publisher struct{}

// New constructs a discard publisher.
func New() *Publisher {
	return &Publisher{}
}

// Name returns the adapter name.
func (p *Publisher) Name() string {
	return Name
}

// Open prepares the publisher for use and validates context cancellation.
func (p *Publisher) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Publish accepts a CloudEvent without transporting it anywhere.
func (p *Publisher) Publish(ctx context.Context, evt cloudevents.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// Close releases publisher resources.
func (p *Publisher) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
