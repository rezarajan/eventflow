package documents

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/lakehouse-poc/internal/lineage"
)

// Projector writes text artifacts for document upload events.
type Projector struct {
	config Config
	store  Store
}

// New constructs a document projector backed by local files.
func New(config Config) *Projector {
	config = config.normalized()
	return NewWithStore(config, NewLocalStore(config.Dir))
}

// NewWithStore constructs a document projector with an injected store for testing.
func NewWithStore(config Config, store Store) *Projector {
	if store == nil {
		config = config.normalized()
		store = NewLocalStore(config.Dir)
	}
	return &Projector{config: config.normalized(), store: store}
}

// Name returns the handler name.
func (p *Projector) Name() string {
	return Name
}

// Dataset returns the stable local text object dataset boundary.
func (p *Projector) Dataset() lineage.Dataset {
	return lineage.FileDataset(p.config.Dir, "documents/")
}

// Open validates projector configuration.
func (p *Projector) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.config.Dir == "" {
		return fmt.Errorf("document directory is required")
	}
	return nil
}

// Handle writes one document artifact when the event represents a text document upload.
func (p *Projector) Handle(ctx context.Context, evt cloudevents.Event) error {
	if evt.Type() != "document.uploaded.v1" {
		return nil
	}
	name, content, err := artifactForEvent(evt)
	if err != nil {
		return err
	}
	if err := p.store.WriteText(ctx, name, content); err != nil {
		return fmt.Errorf("write document artifact %s: %w", name, err)
	}
	return nil
}

// HandleBatch writes document artifacts from a group of CloudEvents.
func (p *Projector) HandleBatch(ctx context.Context, events []cloudevents.Event) error {
	for _, evt := range events {
		if err := p.Handle(ctx, evt); err != nil {
			return err
		}
	}
	return nil
}

// Close releases projector resources.
func (p *Projector) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// artifactForEvent extracts a safe artifact filename and text content from a document event.
func artifactForEvent(evt cloudevents.Event) (string, string, error) {
	data := map[string]any{}
	if len(evt.Data()) > 0 {
		if err := evt.DataAs(&data); err != nil {
			return "", "", fmt.Errorf("decode document event data: %w", err)
		}
	}
	filename := stringField(data, "filename", evt.ID()+".txt")
	filename = safeTextFilename(filename)
	content := stringField(data, "content_preview", "")
	if content == "" {
		content = fmt.Sprintf("Synthetic text document for event %s\n", evt.ID())
	}
	return filename, content, nil
}

// stringField returns a string field from generic event data or a fallback.
func stringField(data map[string]any, key string, fallback string) string {
	value, ok := data[key].(string)
	if !ok || value == "" {
		return fallback
	}
	return value
}

// safeTextFilename normalizes a document filename to a local text artifact name.
func safeTextFilename(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	if base == "." || base == string(filepath.Separator) || base == "" {
		base = "document.txt"
	}
	if filepath.Ext(base) == "" {
		base += ".txt"
	}
	return base
}
