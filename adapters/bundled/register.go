// Package bundled registers Eventflow's in-module resource adapters.
package bundled

import (
	"github.com/rezarajan/eventflow/duckdb"
	"github.com/rezarajan/eventflow/filesystem"
	"github.com/rezarajan/eventflow/httpflow"
	"github.com/rezarajan/eventflow/lineage"
	"github.com/rezarajan/eventflow/redpanda"
	"github.com/rezarajan/eventflow/resource"
	"github.com/rezarajan/eventflow/s3"
)

func Register(catalog *resource.Catalog) error {
	for _, register := range []func(*resource.Catalog) error{
		filesystem.Register,
		httpflow.Register,
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
