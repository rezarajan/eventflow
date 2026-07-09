// Package stdout publishes CloudEvents as JSONL to an io.Writer.
package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

// Name is the adapter name used by fan-out configuration.
const Name = "stdout"

// Publisher writes CloudEvents to an io.Writer.
type Publisher struct {
	writer io.Writer
}

// New constructs a stdout-style publisher with an injected writer.
func New(writer io.Writer) *Publisher {
	return &Publisher{writer: writer}
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

// Publish writes one CloudEvent as JSON followed by a newline.
func (p *Publisher) Publish(ctx context.Context, evt cloudevents.Event) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.writer == nil {
		return fmt.Errorf("stdout writer is required")
	}
	if err := json.NewEncoder(p.writer).Encode(evt); err != nil {
		return fmt.Errorf("encode CloudEvent: %w", err)
	}
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
