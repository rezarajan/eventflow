// Package lineage defines OpenLineage helpers for Eventflow.
package lineage

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"

	eventflow "github.com/rezarajan/eventflow"
	"github.com/rezarajan/eventflow/resource"
)

const (
	// CloudEventType is the Eventflow CloudEvents type for OpenLineage run events.
	CloudEventType = "io.openlineage.run-event.v1"
	// DefaultProducer is used when an OpenLineage event omits producer metadata.
	DefaultProducer = "github.com/rezarajan/eventflow"
	// DefaultSchemaURL is the OpenLineage JSON schema URL used by helper constructors.
	DefaultSchemaURL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
)

// Dataset identifies a stable input or output dataset boundary.
type Dataset struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// Run identifies one executable run.
type Run struct {
	RunID  string         `json:"runId"`
	Facets map[string]any `json:"facets,omitempty"`
}

// Job identifies one executable job.
type Job struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// ParentRunFacet links a child run to its orchestrating parent.
type ParentRunFacet struct {
	Producer  string `json:"_producer"`
	SchemaURL string `json:"_schemaURL"`
	Run       Run    `json:"run"`
	Job       Job    `json:"job"`
}

// Event is an OpenLineage-compatible run event.
type Event struct {
	EventType string    `json:"eventType"`
	EventTime time.Time `json:"eventTime"`
	Run       Run       `json:"run"`
	Job       Job       `json:"job"`
	Inputs    []Dataset `json:"inputs,omitempty"`
	Outputs   []Dataset `json:"outputs,omitempty"`
	Producer  string    `json:"producer"`
	SchemaURL string    `json:"schemaURL"`
	Error     string    `json:"error,omitempty"`
}

// Emitter emits lineage events.
type Emitter interface {
	EmitLineage(ctx context.Context, event Event) error
}

// EmitterSpec is the declarative spec for OpenLineageEmitter.
//
// OpenLineageEmitter wraps another Eventflow emitter and converts native
// OpenLineage events into CloudEvents before delegating to that target.
type EmitterSpec struct {
	EmitterRef Reference `yaml:"emitterRef" json:"emitterRef"`
	Source     string    `yaml:"source,omitempty" json:"source,omitempty"`
}

// Reference aliases resource.Reference for manifest-facing lineage specs.
type Reference = resource.Reference

// OpenLineageEmitter wraps an Eventflow emitter with native OpenLineage support.
type OpenLineageEmitter struct {
	target eventflow.Emitter
	source string
}

// Name returns the adapter name.
func (e *OpenLineageEmitter) Name() string { return "openlineage" }

// Open opens the wrapped target emitter.
func (e *OpenLineageEmitter) Open(ctx context.Context) error { return e.target.Open(ctx) }

// Emit delegates an already-normalized CloudEvent to the wrapped target emitter.
func (e *OpenLineageEmitter) Emit(ctx context.Context, event eventflow.Event) error {
	return e.target.Emit(ctx, event)
}

// Close closes the wrapped target emitter.
func (e *OpenLineageEmitter) Close(ctx context.Context) error { return e.target.Close(ctx) }

// EmitLineage wraps event as a CloudEvent and emits it through the target.
func (e *OpenLineageEmitter) EmitLineage(ctx context.Context, event Event) error {
	wrapped, err := WrapCloudEvent(event, e.source)
	if err != nil {
		return err
	}
	return e.target.Emit(ctx, wrapped)
}

// Register adds the OpenLineageEmitter resource definition.
func Register(catalog *resource.Catalog) error {
	return resource.Register(catalog, resource.Definition[EmitterSpec]{
		GVK: resource.GVK("OpenLineageEmitter"),
		Validate: func(_ context.Context, spec EmitterSpec) error {
			if spec.EmitterRef.Kind == "" || spec.EmitterRef.Name == "" {
				return fmt.Errorf("emitterRef kind and name are required")
			}
			return nil
		},
		References: func(spec EmitterSpec) []resource.Reference {
			ref := spec.EmitterRef
			ref.Capability = resource.CapabilityEmitter
			return []resource.Reference{ref}
		},
		Build: func(_ context.Context, bctx resource.BuildContext, spec EmitterSpec) (any, error) {
			target, err := bctx.Emitter(spec.EmitterRef)
			if err != nil {
				return nil, err
			}
			return &OpenLineageEmitter{target: target, source: spec.Source}, nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityEmitter},
	})
}

// NewEvent constructs a lineage run event.
func NewEvent(eventType string, namespace string, jobName string, runID string, inputs []Dataset, outputs []Dataset, runErr error, now func() time.Time) Event {
	if now == nil {
		now = time.Now
	}
	event := Event{
		EventType: eventType,
		EventTime: now().UTC(),
		Run:       Run{RunID: runID},
		Job:       Job{Namespace: namespace, Name: jobName},
		Inputs:    append([]Dataset(nil), inputs...),
		Outputs:   append([]Dataset(nil), outputs...),
		Producer:  DefaultProducer,
		SchemaURL: DefaultSchemaURL,
	}
	if runErr != nil {
		event.Error = runErr.Error()
	}
	return event
}

// WithParent links a lineage event to a parent run.
func WithParent(event Event, parentJob Job, parentRunID string) Event {
	if parentJob.Name == "" || parentRunID == "" {
		return event
	}
	if event.Run.Facets == nil {
		event.Run.Facets = map[string]any{}
	}
	event.Run.Facets["parent"] = ParentRunFacet{
		Producer:  DefaultProducer,
		SchemaURL: "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
		Run:       Run{RunID: parentRunID},
		Job:       parentJob,
	}
	return event
}

// Validate checks OpenLineage semantics used by Eventflow.
func Validate(event Event) error {
	switch event.EventType {
	case "START", "COMPLETE", "FAIL", "ABORT":
	default:
		return fmt.Errorf("unsupported OpenLineage eventType %q", event.EventType)
	}
	if event.Run.RunID == "" {
		return fmt.Errorf("run.runId is required")
	}
	if event.Job.Namespace == "" || event.Job.Name == "" {
		return fmt.Errorf("job namespace and name are required")
	}
	if event.Producer == "" {
		return fmt.Errorf("producer is required")
	}
	return nil
}

// WrapCloudEvent wraps an OpenLineage run event as a structured CloudEvent.
func WrapCloudEvent(event Event, source string) (eventflow.Event, error) {
	if err := Validate(event); err != nil {
		return eventflow.Event{}, err
	}
	if source == "" {
		source = "urn:eventflow:lineage"
	}
	ce := sdk.NewEvent(sdk.VersionV1)
	ce.SetType(CloudEventType)
	ce.SetSource(source)
	ce.SetSubject(event.Job.Namespace + "/" + event.Job.Name)
	ce.SetTime(event.EventTime)
	if err := ce.SetData(sdk.ApplicationJSON, event); err != nil {
		return eventflow.Event{}, err
	}
	if err := ce.Validate(); err != nil {
		return eventflow.Event{}, err
	}
	return ce, nil
}

// EmitLifecycle emits START and then COMPLETE, FAIL, or ABORT around a completed operation.
func EmitLifecycle(ctx context.Context, emitter Emitter, namespace string, jobName string, runID string, inputs []Dataset, outputs []Dataset, runErr error, now func() time.Time) error {
	if emitter == nil {
		return nil
	}
	if err := emitter.EmitLineage(ctx, NewEvent("START", namespace, jobName, runID, inputs, outputs, nil, now)); err != nil {
		return err
	}
	eventType := "COMPLETE"
	if runErr != nil {
		eventType = "FAIL"
	}
	return emitter.EmitLineage(ctx, NewEvent(eventType, namespace, jobName, runID, inputs, outputs, runErr, now))
}
