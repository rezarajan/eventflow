// Package log publishes CloudEvent metadata to structured logs.
package log

import (
	"context"
	"log/slog"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Name is the adapter name used by fan-out configuration.
const Name = "log"

// Publisher logs event metadata without making storage or network assumptions.
type Publisher struct {
	logger *slog.Logger
}

// New constructs a structured-log publisher.
func New(logger *slog.Logger) *Publisher {
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{logger: logger}
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

// Publish records CloudEvent metadata through structured logging.
func (p *Publisher) Publish(ctx context.Context, evt cloudevents.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	p.logger.Info("event_published", "publisher", Name, "event_id", evt.ID(), "event_type", evt.Type(), "source", evt.Source(), "subject", evt.Subject())
	return nil
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
