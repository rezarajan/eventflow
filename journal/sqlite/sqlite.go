// Package sqlite implements Eventflow's single-node durable journal.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/journal"
	"github.com/rezarajan/eventflow/resource"
)

// Config configures a SQLite journal.
type Config struct {
	Path string
}

// ResourceSpec is the declarative spec for SQLiteJournal.
type ResourceSpec struct {
	Path string `yaml:"path" json:"path"`
}

// Register adds SQLiteJournal to a resource catalog.
func Register(catalog *resource.Catalog) error {
	return resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK: resource.GVK("SQLiteJournal"),
		Default: func(spec *ResourceSpec) error {
			if spec.Path == "" {
				spec.Path = "var/eventflow/journal.sqlite"
			}
			return nil
		},
		Validate: func(_ context.Context, spec ResourceSpec) error {
			if strings.TrimSpace(spec.Path) == "" {
				return fmt.Errorf("path is required")
			}
			return nil
		},
		Build: func(ctx context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return New(Config{Path: spec.Path}), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityJournal},
	})
}

// Journal is a SQLite-backed durable journal.
type Journal struct {
	config Config
	db     *sql.DB
}

// New constructs a SQLite journal.
func New(config Config) *Journal { return &Journal{config: config} }

// Name returns the adapter name.
func (*Journal) Name() string { return "sqlite" }

// Open opens the database and applies the schema.
func (j *Journal) Open(ctx context.Context) error {
	if j.config.Path == "" {
		j.config.Path = "var/eventflow/journal.sqlite"
	}
	if err := os.MkdirAll(filepath.Dir(j.config.Path), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", j.config.Path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	j.db = db
	if _, err := db.ExecContext(ctx, `PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;`); err != nil {
		_ = db.Close()
		j.db = nil
		return err
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		j.db = nil
		return err
	}
	return nil
}

// Append durably stores an accepted event and initializes destination state.
func (j *Journal) Append(ctx context.Context, event eventflow.Event, options journal.AppendOptions) (journal.Record, error) {
	if err := event.Validate(); err != nil {
		return journal.Record{}, eventflow.ValidationError("validate cloudevent", err)
	}
	db, err := j.database()
	if err != nil {
		return journal.Record{}, err
	}
	body, err := json.Marshal(event)
	if err != nil {
		return journal.Record{}, err
	}
	now := time.Now().UTC()
	eventTime := event.Time()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return journal.Record{}, err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `INSERT INTO journal_records(flow,event_id,event_type,source,event_time,body,state,first_seen) VALUES(?,?,?,?,?,?,?,?)`,
		options.Flow, event.ID(), event.Type(), event.Source(), eventTime.Format(time.RFC3339Nano), body, string(journal.StateJournaled), now.Format(time.RFC3339Nano))
	if err != nil {
		return journal.Record{}, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return journal.Record{}, err
	}
	for _, destination := range options.Destinations {
		if strings.TrimSpace(string(destination)) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_deliveries(record_id,destination,state,attempt_count,first_seen) VALUES(?,?,?,?,?)`,
			id, string(destination), string(journal.StatePending), 0, now.Format(time.RFC3339Nano)); err != nil {
			return journal.Record{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return journal.Record{}, err
	}
	return journal.Record{
		ID:        journal.RecordID(id),
		Flow:      options.Flow,
		Event:     event,
		EventID:   event.ID(),
		EventType: event.Type(),
		Source:    event.Source(),
		EventTime: eventTime,
		FirstSeen: now,
		State:     journal.StateJournaled,
	}, nil
}

// MarkAttempt records a dispatch attempt and claims a destination briefly.
func (j *Journal) MarkAttempt(ctx context.Context, recordID journal.RecordID, destination journal.DestinationID, nextAttempt time.Time) error {
	db, err := j.database()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	_, err = db.ExecContext(ctx, `UPDATE journal_deliveries SET state=?, attempt_count=attempt_count+1, last_attempt=?, next_attempt=? WHERE record_id=? AND destination=?`,
		string(journal.StateDelivering), now.Format(time.RFC3339Nano), formatTime(nextAttempt), int64(recordID), string(destination))
	return err
}

// MarkDelivered records successful delivery.
func (j *Journal) MarkDelivered(ctx context.Context, recordID journal.RecordID, destination journal.DestinationID) error {
	db, err := j.database()
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, `UPDATE journal_deliveries SET state=?, last_error='', next_attempt='' WHERE record_id=? AND destination=?`,
		string(journal.StateDelivered), int64(recordID), string(destination))
	return err
}

// MarkFailed records a retryable or terminal delivery failure.
func (j *Journal) MarkFailed(ctx context.Context, recordID journal.RecordID, destination journal.DestinationID, cause error, terminal bool, nextAttempt time.Time) error {
	db, err := j.database()
	if err != nil {
		return err
	}
	state := journal.StatePending
	if terminal {
		state = journal.StateFailed
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err = db.ExecContext(ctx, `UPDATE journal_deliveries SET state=?, last_error=?, next_attempt=? WHERE record_id=? AND destination=?`,
		string(state), message, formatTime(nextAttempt), int64(recordID), string(destination))
	return err
}

// Pending returns destination rows ready for dispatch.
func (j *Journal) Pending(ctx context.Context, filter journal.PendingFilter) ([]journal.Delivery, error) {
	db, err := j.database()
	if err != nil {
		return nil, err
	}
	now := filter.Now
	if now.IsZero() {
		now = time.Now().UTC()
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 100
	}
	rows, err := db.QueryContext(ctx, `SELECT d.record_id,d.destination,d.state,d.attempt_count,d.last_error,d.first_seen,d.last_attempt,d.next_attempt
FROM journal_deliveries d JOIN journal_records r ON r.id=d.record_id
WHERE d.state IN (?,?) AND (?='' OR r.flow=?) AND (d.next_attempt='' OR d.next_attempt<=?)
ORDER BY d.record_id, d.destination LIMIT ?`,
		string(journal.StatePending), string(journal.StateDelivering), filter.Flow, filter.Flow, now.Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []journal.Delivery
	for rows.Next() {
		delivery, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, delivery)
	}
	return out, rows.Err()
}

// Get loads one record by ID.
func (j *Journal) Get(ctx context.Context, recordID journal.RecordID) (journal.Record, error) {
	db, err := j.database()
	if err != nil {
		return journal.Record{}, err
	}
	row := db.QueryRowContext(ctx, `SELECT id,flow,event_id,event_type,source,event_time,body,state,first_seen FROM journal_records WHERE id=?`, int64(recordID))
	return scanRecord(row)
}

// Query selects records for replay.
func (j *Journal) Query(ctx context.Context, filter journal.ReplayFilter) (journal.Iterator, error) {
	db, err := j.database()
	if err != nil {
		return nil, err
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 1000
	}
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT r.id,r.flow,r.event_id,r.event_type,r.source,r.event_time,r.body,r.state,r.first_seen
FROM journal_records r LEFT JOIN journal_deliveries d ON d.record_id=r.id
WHERE (?='' OR r.flow=?)
  AND (?='' OR r.event_id=?)
  AND (?='' OR d.destination=?)
  AND (?='' OR d.state=?)
  AND (?='' OR r.first_seen>=?)
  AND (?='' OR r.first_seen<=?)
ORDER BY r.id LIMIT ?`,
		filter.Flow, filter.Flow,
		filter.EventID, filter.EventID,
		string(filter.Destination), string(filter.Destination),
		string(filter.State), string(filter.State),
		formatTime(filter.Since), formatTime(filter.Since),
		formatTime(filter.Until), formatTime(filter.Until),
		limit)
	if err != nil {
		return nil, err
	}
	return &iterator{rows: rows}, nil
}

// Close closes the database.
func (j *Journal) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if j.db == nil {
		return nil
	}
	err := j.db.Close()
	j.db = nil
	return err
}

func (j *Journal) database() (*sql.DB, error) {
	if j.db == nil {
		return nil, fmt.Errorf("sqlite journal is not open")
	}
	return j.db, nil
}

type iterator struct{ rows *sql.Rows }

func (i *iterator) Next(ctx context.Context) (journal.Record, error) {
	if err := ctx.Err(); err != nil {
		return journal.Record{}, err
	}
	if !i.rows.Next() {
		if err := i.rows.Err(); err != nil {
			return journal.Record{}, err
		}
		return journal.Record{}, io.EOF
	}
	return scanRecord(i.rows)
}

func (i *iterator) Close() error { return i.rows.Close() }

type scanner interface {
	Scan(dest ...any) error
}

func scanRecord(row scanner) (journal.Record, error) {
	var record journal.Record
	var eventTime, firstSeen, state string
	var body []byte
	if err := row.Scan(&record.ID, &record.Flow, &record.EventID, &record.EventType, &record.Source, &eventTime, &body, &state, &firstSeen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return journal.Record{}, journal.ErrNotFound
		}
		return journal.Record{}, err
	}
	if err := json.Unmarshal(body, &record.Event); err != nil {
		return journal.Record{}, err
	}
	record.EventTime = parseTime(eventTime)
	record.FirstSeen = parseTime(firstSeen)
	record.State = journal.State(state)
	return record, nil
}

func scanDelivery(rows *sql.Rows) (journal.Delivery, error) {
	var delivery journal.Delivery
	var firstSeen, lastAttempt, nextAttempt string
	if err := rows.Scan(&delivery.RecordID, &delivery.Destination, &delivery.State, &delivery.AttemptCount, &delivery.LastError, &firstSeen, &lastAttempt, &nextAttempt); err != nil {
		return journal.Delivery{}, err
	}
	delivery.FirstSeen = parseTime(firstSeen)
	delivery.LastAttempt = parseTime(lastAttempt)
	delivery.NextAttempt = parseTime(nextAttempt)
	return delivery, nil
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	t, _ := time.Parse(time.RFC3339Nano, value)
	return t
}

const schema = `
CREATE TABLE IF NOT EXISTS journal_records (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	flow TEXT NOT NULL,
	event_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	source TEXT NOT NULL,
	event_time TEXT NOT NULL,
	body BLOB NOT NULL,
	state TEXT NOT NULL,
	first_seen TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS journal_deliveries (
	record_id INTEGER NOT NULL,
	destination TEXT NOT NULL,
	state TEXT NOT NULL,
	attempt_count INTEGER NOT NULL DEFAULT 0,
	last_error TEXT NOT NULL DEFAULT '',
	first_seen TEXT NOT NULL,
	last_attempt TEXT NOT NULL DEFAULT '',
	next_attempt TEXT NOT NULL DEFAULT '',
	PRIMARY KEY(record_id, destination),
	FOREIGN KEY(record_id) REFERENCES journal_records(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_journal_records_flow ON journal_records(flow);
CREATE INDEX IF NOT EXISTS idx_journal_records_event_id ON journal_records(event_id);
CREATE INDEX IF NOT EXISTS idx_journal_deliveries_state ON journal_deliveries(state, next_attempt);
`
