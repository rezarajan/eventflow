package duckdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/rezarajan/eventflow/internal/lineage"
)

// Projector materializes CloudEvents into an Eventflow-owned raw DuckDB table.
type Projector struct {
	config Config
	db     *sql.DB
}

// New constructs a DuckDB projector backed by a local DuckDB file.
func New(config Config) *Projector {
	return &Projector{config: config.normalized()}
}

// NewWithDB constructs a DuckDB projector with an injected database handle.
func NewWithDB(config Config, db *sql.DB) *Projector {
	return &Projector{config: config.normalized(), db: db}
}

// Name returns the handler name.
func (p *Projector) Name() string {
	return Name
}

// Dataset returns the stable DuckDB dataset boundary.
func (p *Projector) Dataset() lineage.Dataset {
	return lineage.DuckDBDataset(p.config.Path, "tables/")
}

// OutputDatasets returns stable DuckDB table datasets written by this projector.
func (p *Projector) OutputDatasets() []lineage.Dataset {
	return []lineage.Dataset{lineage.DuckDBDataset(p.config.Path, "_raw_events")}
}

// Open initializes the DuckDB database and required tables.
func (p *Projector) Open(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.config.Path == "" {
		return fmt.Errorf("duckdb path is required")
	}
	if p.db == nil {
		if p.config.Path != ":memory:" {
			if err := os.MkdirAll(filepath.Dir(p.config.Path), 0o755); err != nil {
				return err
			}
		}
		db, err := sql.Open("duckdb", p.config.Path)
		if err != nil {
			return fmt.Errorf("open DuckDB %s: %w", p.config.Path, err)
		}
		p.db = db
	}
	if err := p.createTables(ctx); err != nil {
		return err
	}
	return nil
}

// Handle materializes one CloudEvent.
func (p *Projector) Handle(ctx context.Context, evt cloudevents.Event) error {
	return p.HandleBatch(ctx, []cloudevents.Event{evt})
}

// HandleBatch writes CloudEvents to DuckDB in one transaction.
func (p *Projector) HandleBatch(ctx context.Context, events []cloudevents.Event) error {
	if len(events) == 0 {
		return nil
	}
	if p.db == nil {
		return fmt.Errorf("duckdb projector must be opened before handling events")
	}
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin DuckDB transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	for _, evt := range events {
		row, err := rowForEvent(evt)
		if err != nil {
			return err
		}
		if err := insertRow(ctx, tx, "_raw_events", row); err != nil {
			return fmt.Errorf("insert raw DuckDB event %s: %w", evt.ID(), err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit DuckDB transaction: %w", err)
	}
	committed = true
	return nil
}

// Close releases the DuckDB database handle.
func (p *Projector) Close(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if p.db == nil {
		return nil
	}
	err := p.db.Close()
	p.db = nil
	return err
}

// createTables creates the raw event table.
func (p *Projector) createTables(ctx context.Context) error {
	return createTable(ctx, p.db, "_raw_events")
}

// createTable creates one idempotent CloudEvents projection table.
func createTable(ctx context.Context, db *sql.DB, table string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		event_id VARCHAR PRIMARY KEY,
		event_type VARCHAR NOT NULL,
		event_source VARCHAR NOT NULL,
		event_subject VARCHAR,
		event_time TIMESTAMP,
		data_json VARCHAR NOT NULL
	)`, quoteIdent(table)))
	if err != nil {
		return fmt.Errorf("create DuckDB table %s: %w", table, err)
	}
	return nil
}

// eventRow is the common row shape used for raw and typed DuckDB tables.
type eventRow struct {
	ID       string
	Type     string
	Source   string
	Subject  string
	Time     time.Time
	DataJSON string
}

// rowForEvent converts a CloudEvent into the DuckDB row shape.
func rowForEvent(evt cloudevents.Event) (eventRow, error) {
	if err := evt.Validate(); err != nil {
		return eventRow{}, fmt.Errorf("validate CloudEvent for DuckDB: %w", err)
	}
	data := evt.Data()
	if len(data) == 0 {
		data = []byte("{}")
	}
	if !json.Valid(data) {
		return eventRow{}, fmt.Errorf("CloudEvent %s data is not valid JSON", evt.ID())
	}
	return eventRow{
		ID:       evt.ID(),
		Type:     evt.Type(),
		Source:   evt.Source(),
		Subject:  evt.Subject(),
		Time:     evt.Time().UTC(),
		DataJSON: string(data),
	}, nil
}

// insertRow inserts one row while preserving idempotency by event_id.
func insertRow(ctx context.Context, tx *sql.Tx, table string, row eventRow) error {
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`INSERT OR IGNORE INTO %s
		(event_id, event_type, event_source, event_subject, event_time, data_json)
		VALUES (?, ?, ?, ?, ?, ?)`, quoteIdent(table)),
		row.ID, row.Type, row.Source, row.Subject, row.Time, row.DataJSON,
	)
	return err
}

// quoteIdent quotes a trusted DuckDB identifier.
func quoteIdent(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}
