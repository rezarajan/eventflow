// Package bundled registers Eventflow's in-module resource adapters.
package bundled

import (
	"github.com/rezarajan/eventflow/duckdb"
	"github.com/rezarajan/eventflow/filesystem"
	"github.com/rezarajan/eventflow/httpflow"
	sqlitejournal "github.com/rezarajan/eventflow/journal/sqlite"
	"github.com/rezarajan/eventflow/lineage"
	"github.com/rezarajan/eventflow/redpanda"
	"github.com/rezarajan/eventflow/resource"
	"github.com/rezarajan/eventflow/s3"
)

// Register adds all adapters shipped in this module to catalog.
//
// Applications that need a smaller surface can call individual adapter package
// Register functions instead.
func Register(catalog *resource.Catalog) error {
	for _, register := range []func(*resource.Catalog) error{
		filesystem.Register,
		httpflow.Register,
		sqlitejournal.Register,
		redpanda.Register,
		s3.Register,
		duckdb.Register,
		lineage.Register,
	} {
		if err := register(catalog); err != nil {
			return err
		}
	}
	return nil
}
