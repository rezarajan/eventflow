package jsonl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/datascape/eventflow/internal/lineage"
)

// Projector materializes selected domain events into simple JSONL tables.
type Projector struct {
	config Config
	store  Store
}

// New constructs a JSONL projector backed by local files.
func New(config Config) *Projector {
	config = config.normalized()
	return NewWithStore(config, NewLocalStore(config.Dir))
}

// NewWithStore constructs a JSONL projector with an injected store for testing.
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

// Dataset returns the stable materialized JSONL dataset boundary.
func (p *Projector) Dataset() lineage.Dataset {
	return lineage.FileDataset(p.config.Dir, "tables/")
}

// Open validates projector configuration.
func (p *Projector) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.config.Dir == "" {
		return fmt.Errorf("jsonl directory is required")
	}
	return nil
}

// OutputDatasets returns stable dataset identifiers for each JSONL table.
func (p *Projector) OutputDatasets() []lineage.Dataset {
	return []lineage.Dataset{
		lineage.FileDataset(p.config.Dir, "_raw_events.jsonl"),
	}
}

// Handle materializes one CloudEvent.
func (p *Projector) Handle(ctx context.Context, evt cloudevents.Event) error {
	return p.HandleBatch(ctx, []cloudevents.Event{evt})
}

// HandleBatch materializes a group of CloudEvents with table-level writes.
func (p *Projector) HandleBatch(ctx context.Context, events []cloudevents.Event) error {
	lines := make([][]byte, 0, len(events))
	for _, evt := range events {
		line, err := rowForEvent(evt)
		if err != nil {
			return err
		}
		lines = append(lines, line)
	}
	if err := p.store.AppendLines(ctx, "_raw_events.jsonl", lines); err != nil {
		return fmt.Errorf("append JSONL table _raw_events.jsonl: %w", err)
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

// rowForEvent converts one CloudEvent into a JSON-compatible materialized row.
func rowForEvent(evt cloudevents.Event) ([]byte, error) {
	data := map[string]any{}
	if len(evt.Data()) > 0 {
		if err := evt.DataAs(&data); err != nil {
			return nil, fmt.Errorf("decode CloudEvent data for JSONL row: %w", err)
		}
	}
	row := map[string]any{
		"event_id":      evt.ID(),
		"event_type":    evt.Type(),
		"event_source":  evt.Source(),
		"event_subject": evt.Subject(),
		"event_time":    evt.Time().UTC().Format(time.RFC3339Nano),
	}
	for key, value := range data {
		row[key] = value
	}
	body, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("marshal JSONL row: %w", err)
	}
	return body, nil
}
