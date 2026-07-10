// Package consume defines event source and handler ports for CloudEvents consumers.
package consume

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/rezarajan/project-datascape/internal/lineage"
)

// EventSource reads CloudEvents from an external event stream through an explicit lifecycle.
type EventSource interface {
	Name() string
	Dataset() lineage.Dataset
	Open(ctx context.Context) error
	ReadBatch(ctx context.Context, maxEvents int) ([]cloudevents.Event, error)
	Close(ctx context.Context) error
}

// EventHandler handles one CloudEvent through an explicit lifecycle.
type EventHandler interface {
	Name() string
	Dataset() lineage.Dataset
	Open(ctx context.Context) error
	Handle(ctx context.Context, event cloudevents.Event) error
	Close(ctx context.Context) error
}

// BatchEventHandler handles groups of CloudEvents when an adapter can apply them efficiently.
type BatchEventHandler interface {
	EventHandler
	HandleBatch(ctx context.Context, events []cloudevents.Event) error
}

// OutputDatasetProvider exposes precise output datasets for lineage.
type OutputDatasetProvider interface {
	OutputDatasets() []lineage.Dataset
}

// Factory creates named event handlers from configuration.
type Factory interface {
	Names() []string
	Create(name string) (EventHandler, error)
}
