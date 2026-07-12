// Package spool implements explicit single-instance SQLite spooling for OpenLineage admission gateways.
package spool

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	eventflow "github.com/rezarajan/eventflow"
)

type Record struct {
	ID      int64
	Event   eventflow.Event
	Created time.Time
}

type SQLite struct {
	path string
	db   *sql.DB
}

func NewSQLite(path string) *SQLite { return &SQLite{path: path} }

func (s *SQLite) Open(ctx context.Context) error {
	if s.path == "" {
		s.path = "var/eventflow/spool.sqlite"
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o755); err != nil {
		return err
	}
	db, err := sql.Open("sqlite", s.path)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	s.db = db
	_, err = db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS spool_records (id INTEGER PRIMARY KEY AUTOINCREMENT, event BLOB NOT NULL, created TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);`)
	return err
}

func (s *SQLite) Close(context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *SQLite) Put(ctx context.Context, event eventflow.Event) error {
	if s.db == nil {
		return fmt.Errorf("spool is not open")
	}
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO spool_records(event,created) VALUES(?,?)`, body, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *SQLite) Pending(ctx context.Context, limit int) ([]Record, error) {
	if s.db == nil {
		return nil, fmt.Errorf("spool is not open")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,event,created FROM spool_records WHERE delivered=0 ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var record Record
		var body []byte
		var created string
		if err := rows.Scan(&record.ID, &body, &created); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(body, &record.Event); err != nil {
			return nil, err
		}
		record.Created, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, record)
	}
	return out, rows.Err()
}

func (s *SQLite) MarkDelivered(ctx context.Context, id int64) error {
	if s.db == nil {
		return fmt.Errorf("spool is not open")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE spool_records SET delivered=1 WHERE id=?`, id)
	return err
}

func (s *SQLite) Depth(ctx context.Context) (int, error) {
	if s.db == nil {
		return 0, nil
	}
	var depth int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM spool_records WHERE delivered=0`).Scan(&depth)
	return depth, err
}
