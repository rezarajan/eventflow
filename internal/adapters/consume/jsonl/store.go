package jsonl

import (
	"context"
	"os"
	"path/filepath"
)

// Store appends newline-delimited rows to materialized table paths.
type Store interface {
	AppendLines(ctx context.Context, table string, lines [][]byte) error
}

// LocalStore writes materialized JSONL tables to a local directory.
type LocalStore struct {
	dir string
}

// NewLocalStore constructs a local JSONL store.
func NewLocalStore(dir string) *LocalStore {
	return &LocalStore{dir: dir}
}

// AppendLines appends JSON lines to one table file, creating parent directories as needed.
func (s *LocalStore) AppendLines(ctx context.Context, table string, lines [][]byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if len(lines) == 0 {
		return nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.dir, filepath.Base(table))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, line := range lines {
		if _, err := file.Write(line); err != nil {
			return err
		}
		if _, err := file.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}
