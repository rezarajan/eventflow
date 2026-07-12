// Package quarantine implements OpenLineage admission quarantine storage for shared data-platform infrastructure.
package quarantine

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	eventflow "github.com/rezarajan/eventflow"
)

type Status string

const (
	StatusOpen      Status = "OPEN"
	StatusDismissed Status = "DISMISSED"
	StatusReplayed  Status = "REPLAYED"
)

type Record struct {
	ID               int64              `json:"id"`
	Raw              []byte             `json:"raw,omitempty"`
	Digest           string             `json:"digest"`
	CloudEventsID    string             `json:"cloudEventsId,omitempty"`
	OpenLineageRunID string             `json:"openLineageRunId,omitempty"`
	Principal        string             `json:"principal"`
	PolicyName       string             `json:"policyName"`
	PolicyVersion    string             `json:"policyVersion"`
	ContractName     string             `json:"contractName"`
	ContractVersion  string             `json:"contractVersion"`
	Decision         eventflow.Decision `json:"decision"`
	Field            string             `json:"field,omitempty"`
	ReceiveTime      time.Time          `json:"receiveTime"`
	Target           string             `json:"target"`
	Status           Status             `json:"status"`
	ReplayCount      int                `json:"replayCount"`
}

type Store struct {
	path string
	db   *sql.DB
}

func New(path string) *Store { return &Store{path: path} }

func (s *Store) Open(ctx context.Context) error {
	if s.path == "" {
		s.path = "var/eventflow/quarantine.sqlite"
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
	_, err = db.ExecContext(ctx, schema)
	return err
}

func (s *Store) Close(context.Context) error {
	if s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) Put(ctx context.Context, record Record) (Record, error) {
	if s.db == nil {
		return Record{}, fmt.Errorf("quarantine store is not open")
	}
	if record.ReceiveTime.IsZero() {
		record.ReceiveTime = time.Now().UTC()
	}
	sum := sha256.Sum256(record.Raw)
	record.Digest = hex.EncodeToString(sum[:])
	if record.Status == "" {
		record.Status = StatusOpen
	}
	decision, err := json.Marshal(record.Decision)
	if err != nil {
		return Record{}, err
	}
	result, err := s.db.ExecContext(ctx, `INSERT INTO quarantine_records(raw,digest,ce_id,run_id,principal,policy_name,policy_version,contract_name,contract_version,decision,field,receive_time,target,status,replay_count)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		record.Raw, record.Digest, record.CloudEventsID, record.OpenLineageRunID, record.Principal, record.PolicyName, record.PolicyVersion,
		record.ContractName, record.ContractVersion, decision, record.Field, record.ReceiveTime.Format(time.RFC3339Nano), record.Target, string(record.Status), record.ReplayCount)
	if err != nil {
		return Record{}, err
	}
	record.ID, err = result.LastInsertId()
	return record, err
}

func (s *Store) Get(ctx context.Context, id int64) (Record, error) {
	if s.db == nil {
		return Record{}, fmt.Errorf("quarantine store is not open")
	}
	row := s.db.QueryRowContext(ctx, `SELECT id,raw,digest,ce_id,run_id,principal,policy_name,policy_version,contract_name,contract_version,decision,field,receive_time,target,status,replay_count FROM quarantine_records WHERE id=?`, id)
	return scan(row)
}

func (s *Store) List(ctx context.Context, status Status, limit int) ([]Record, error) {
	if s.db == nil {
		return nil, fmt.Errorf("quarantine store is not open")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,raw,digest,ce_id,run_id,principal,policy_name,policy_version,contract_name,contract_version,decision,field,receive_time,target,status,replay_count FROM quarantine_records WHERE (?='' OR status=?) ORDER BY id LIMIT ?`, string(status), string(status), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []Record
	for rows.Next() {
		record, err := scan(rows)
		if err != nil {
			return nil, err
		}
		record.Raw = nil
		records = append(records, record)
	}
	return records, rows.Err()
}

func (s *Store) Dismiss(ctx context.Context, id int64) error {
	return s.setStatus(ctx, id, StatusDismissed)
}

func (s *Store) MarkReplayed(ctx context.Context, id int64, principal string) error {
	if s.db == nil {
		return fmt.Errorf("quarantine store is not open")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := s.db.ExecContext(ctx, `UPDATE quarantine_records SET status=?, replay_count=replay_count+1 WHERE id=?`, string(StatusReplayed), id); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO quarantine_replay(record_id,principal,replay_time) VALUES(?,?,?)`, id, principal, now)
	return err
}

func (s *Store) setStatus(ctx context.Context, id int64, status Status) error {
	if s.db == nil {
		return fmt.Errorf("quarantine store is not open")
	}
	_, err := s.db.ExecContext(ctx, `UPDATE quarantine_records SET status=? WHERE id=?`, string(status), id)
	return err
}

type scanner interface{ Scan(...any) error }

func scan(row scanner) (Record, error) {
	var record Record
	var decision []byte
	var receiveTime, status string
	err := row.Scan(&record.ID, &record.Raw, &record.Digest, &record.CloudEventsID, &record.OpenLineageRunID, &record.Principal,
		&record.PolicyName, &record.PolicyVersion, &record.ContractName, &record.ContractVersion, &decision, &record.Field,
		&receiveTime, &record.Target, &status, &record.ReplayCount)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, err
	}
	if err != nil {
		return Record{}, err
	}
	_ = json.Unmarshal(decision, &record.Decision)
	record.ReceiveTime, _ = time.Parse(time.RFC3339Nano, receiveTime)
	record.Status = Status(status)
	return record, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS quarantine_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  raw BLOB NOT NULL,
  digest TEXT NOT NULL,
  ce_id TEXT NOT NULL DEFAULT '',
  run_id TEXT NOT NULL DEFAULT '',
  principal TEXT NOT NULL DEFAULT '',
  policy_name TEXT NOT NULL DEFAULT '',
  policy_version TEXT NOT NULL DEFAULT '',
  contract_name TEXT NOT NULL DEFAULT '',
  contract_version TEXT NOT NULL DEFAULT '',
  decision TEXT NOT NULL,
  field TEXT NOT NULL DEFAULT '',
  receive_time TEXT NOT NULL,
  target TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  replay_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS quarantine_replay (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  record_id INTEGER NOT NULL,
  principal TEXT NOT NULL,
  replay_time TEXT NOT NULL
);`
