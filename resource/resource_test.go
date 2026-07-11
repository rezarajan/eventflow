package resource

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

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

func docsFromYAML(t *testing.T, body string) []Document {
	t.Helper()
	docs, err := loadBytes([]byte(body), "test")
	if err != nil {
		t.Fatal(err)
	}
	return docs
}

func resourceGVK(kind string) GroupVersionKind { return GVK(kind) }
