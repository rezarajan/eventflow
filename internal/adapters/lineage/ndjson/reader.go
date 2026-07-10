// Package ndjson reads OpenLineage events from newline-delimited JSON.
package ndjson

import "github.com/rezarajan/project-datascape/internal/lineage"

// NewFileReader opens an NDJSON lineage file.
func NewFileReader(path string) (lineage.EventReader, error) {
	return lineage.NewNDJSONFileReader(path)
}
