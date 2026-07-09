package documents

import (
	"context"
	"os"
	"path/filepath"
)

// Store writes document artifacts by relative filename.
type Store interface {
	WriteText(ctx context.Context, name string, content string) error
}

// LocalStore writes document artifacts to a local directory.
type LocalStore struct {
	dir string
}

// NewLocalStore constructs a local document store.
func NewLocalStore(dir string) *LocalStore {
	return &LocalStore{dir: dir}
}

// WriteText writes one text artifact, creating parent directories as needed.
func (s *LocalStore) WriteText(ctx context.Context, name string, content string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.dir, filepath.Base(name))
	return os.WriteFile(path, []byte(content), 0o644)
}
