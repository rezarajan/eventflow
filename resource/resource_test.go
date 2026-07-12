package resource

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	sdk "github.com/cloudevents/sdk-go/v2"
	eventflow "github.com/rezarajan/eventflow"
)

type testEmitter struct{ events []eventflow.Event }

func (*testEmitter) Open(context.Context) error { return nil }
func (e *testEmitter) Emit(_ context.Context, event eventflow.Event) error {
	e.events = append(e.events, event)
	return nil
}
func (*testEmitter) Close(context.Context) error { return nil }

type testReceiver struct{}

func (*testReceiver) Open(context.Context) error                       { return nil }
func (*testReceiver) Receive(context.Context) (eventflow.Event, error) { return eventflow.Event{}, nil }
func (*testReceiver) Close(context.Context) error                      { return nil }

type testObserver struct{}

func (*testObserver) Open(context.Context) error { return nil }
func (*testObserver) Observe(context.Context) (eventflow.Observation, error) {
	return eventflow.Observation{}, io.EOF
}
func (*testObserver) Close(context.Context) error { return nil }

type testMapper struct{}

func (*testMapper) MapObservation(context.Context, eventflow.Observation) (eventflow.Event, error) {
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID("mapped")
	event.SetType("example.observed")
	event.SetSource("test")
	event.SetTime(time.Date(2026, 7, 11, 0, 0, 0, 0, time.UTC))
	if err := event.SetData("application/json", map[string]string{"ok": "yes"}); err != nil {
		return eventflow.Event{}, err
	}
	return event, nil
}

type testSpec struct {
	Value string `yaml:"value"`
}

func TestLoadFilesMultiDocument(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resources.yaml")
	if err := os.WriteFile(path, []byte(`apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: one
spec:
  type: example.one
---
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: two
spec:
  type: example.two
`), 0o644); err != nil {
		t.Fatal(err)
	}
	docs, err := LoadFiles(path)
	if err != nil {
		t.Fatalf("LoadFiles() error = %v", err)
	}
	if len(docs) != 2 {
		t.Fatalf("len(docs) = %d, want 2", len(docs))
	}
}

func TestValidateRejectsUnknownSpecField(t *testing.T) {
	catalog := NewCatalog()
	if err := Register(catalog, Definition[testSpec]{GVK: GVK("Test")}); err != nil {
		t.Fatal(err)
	}
	docs, err := loadBytes([]byte(`apiVersion: eventflow.dev/v1alpha1
kind: Test
metadata:
  name: bad
spec:
  value: ok
  extra: no
`), "test")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Validate(context.Background(), catalog, docs)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("Validate() error = %v, want ErrValidation", err)
	}
}

func TestValidateMissingReferenceAndCapabilityMismatch(t *testing.T) {
	catalog := NewCatalog()
	if err := Register(catalog, Definition[testSpec]{
		GVK:          GVK("Thing"),
		Capabilities: []Capability{CapabilityComponent},
	}); err != nil {
		t.Fatal(err)
	}
	missing := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: flow
spec:
  receiverRef: {kind: FilesystemReceiver, name: in}
  emitterRefs:
    - {kind: Thing, name: missing}
`)
	_, err := Validate(context.Background(), catalog, missing)
	if !errors.Is(err, ErrMissingReference) {
		t.Fatalf("missing ref error = %v, want ErrMissingReference", err)
	}
	mismatch := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: Thing
metadata: {name: out}
spec:
  value: ok
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: flow
spec:
  receiverRef: {kind: Thing, name: out}
  emitterRefs:
    - {kind: Thing, name: out}
`)
	_, err = Validate(context.Background(), catalog, mismatch)
	if !errors.Is(err, ErrCapabilityMismatch) {
		t.Fatalf("capability error = %v, want ErrCapabilityMismatch", err)
	}
}

func TestCompileEventFlow(t *testing.T) {
	catalog := NewCatalog()
	if err := Register(catalog, Definition[testSpec]{
		GVK: resourceGVK("TestEmitter"),
		Build: func(context.Context, BuildContext, testSpec) (any, error) {
			return &testEmitter{}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityEmitter},
	}); err != nil {
		t.Fatal(err)
	}
	if err := Register(catalog, Definition[testSpec]{
		GVK: resourceGVK("TestReceiver"),
		Build: func(context.Context, BuildContext, testSpec) (any, error) {
			return &testReceiver{}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityReceiver},
	}); err != nil {
		t.Fatal(err)
	}
	docs := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: TestReceiver
metadata: {name: in}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: TestEmitter
metadata: {name: out}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata: {name: contract}
spec:
  type: example.created
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata: {name: flow}
spec:
  receiverRef: {kind: TestReceiver, name: in}
  contractRefs:
    - {kind: EventContract, name: contract}
  emitterRefs:
    - {kind: TestEmitter, name: out}
`)
	compiled, err := Compile(context.Background(), catalog, docs)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(compiled.Flows) != 1 {
		t.Fatalf("flows = %d, want 1", len(compiled.Flows))
	}
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID("1")
	event.SetType("example.created")
	event.SetSource("test")
	if err := event.SetData("application/json", map[string]string{"ok": "yes"}); err != nil {
		t.Fatal(err)
	}
	if err := compiled.Flows[0].Runtime.Validator.Validate(context.Background(), event, eventflow.ValidationStrict); err != nil {
		t.Fatalf("Validate event = %v", err)
	}
}

func TestContractValidatesJSONSchemaPayload(t *testing.T) {
	dir := t.TempDir()
	schema := filepath.Join(dir, "payload.schema.json")
	if err := os.WriteFile(schema, []byte(`{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	validator := contractValidator{contracts: []EventContractSpec{{Type: "example.created", DataSchema: schema}}}
	event := sdk.NewEvent(sdk.VersionV1)
	event.SetID("1")
	event.SetType("example.created")
	event.SetSource("test")
	if err := event.SetData("application/json", map[string]any{"missing": "id"}); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), event, eventflow.ValidationStrict); !errors.Is(err, eventflow.ErrValidation) {
		t.Fatalf("Validate() error = %v, want validation error", err)
	}
	if err := event.SetData("application/json", map[string]any{"id": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(context.Background(), event, eventflow.ValidationStrict); err != nil {
		t.Fatalf("Validate() valid event error = %v", err)
	}
}

func TestEventFlowRejectsObserverRef(t *testing.T) {
	docs := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata: {name: flow}
spec:
  observerRef: {kind: TestObserver, name: obs}
  emitterRefs:
    - {kind: TestEmitter, name: out}
`)
	_, err := Validate(context.Background(), NewCatalog(), docs)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("Validate() error = %v, want ErrValidation", err)
	}
}

func TestCompileObservationFlow(t *testing.T) {
	catalog := NewCatalog()
	if err := Register(catalog, Definition[testSpec]{
		GVK: resourceGVK("TestEmitter"),
		Build: func(context.Context, BuildContext, testSpec) (any, error) {
			return &testEmitter{}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityEmitter},
	}); err != nil {
		t.Fatal(err)
	}
	if err := Register(catalog, Definition[testSpec]{
		GVK: resourceGVK("TestObserver"),
		Build: func(context.Context, BuildContext, testSpec) (any, error) {
			return &testObserver{}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityObserver},
	}); err != nil {
		t.Fatal(err)
	}
	if err := Register(catalog, Definition[testSpec]{
		GVK: resourceGVK("TestMapper"),
		Build: func(context.Context, BuildContext, testSpec) (any, error) {
			return &testMapper{}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityObservationMapper},
	}); err != nil {
		t.Fatal(err)
	}
	docs := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: TestObserver
metadata: {name: obs}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: TestMapper
metadata: {name: mapper}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: TestEmitter
metadata: {name: out}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata: {name: contract}
spec:
  type: example.observed
---
apiVersion: eventflow.dev/v1alpha1
kind: ObservationFlow
metadata: {name: observed}
spec:
  observerRef: {kind: TestObserver, name: obs}
  mapperRef: {kind: TestMapper, name: mapper}
  contractRefs:
    - {kind: EventContract, name: contract}
  emitterRefs:
    - {kind: TestEmitter, name: out}
`)
	compiled, err := Compile(context.Background(), catalog, docs)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if len(compiled.ObservationFlows) != 1 {
		t.Fatalf("observation flows = %d, want 1", len(compiled.ObservationFlows))
	}
	observation := eventflow.Observation{Attributes: map[string]string{"ok": "yes"}}
	event, err := compiled.ObservationFlows[0].Runtime.Mapper.MapObservation(context.Background(), observation)
	if err != nil {
		t.Fatalf("MapObservation() error = %v", err)
	}
	if event.Type() != "example.observed" {
		t.Fatalf("mapped event type = %s, want example.observed", event.Type())
	}
}

func docsFromYAML(t *testing.T, body string) []Document {
	t.Helper()
	docs, err := loadBytes([]byte(body), "test")
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func resourceGVK(kind string) GroupVersionKind { return GVK(kind) }
