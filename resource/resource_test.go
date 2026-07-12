package resource

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type testSpec struct {
	Value string `yaml:"value"`
}

func TestLoadFilesMultiDocument(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resources.yaml")
	if err := os.WriteFile(path, []byte(`apiVersion: eventflow.dev/v1alpha1
kind: OpenLineageContract
metadata: {name: one}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: OpenLineagePolicy
metadata: {name: policy}
spec: {}
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
	docs := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: Test
metadata: {name: bad}
spec:
  value: ok
  extra: no
`)
	_, err := Validate(context.Background(), catalog, docs)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("Validate() error = %v, want ErrValidation", err)
	}
}

func TestValidateMissingReferenceAndCapabilityMismatch(t *testing.T) {
	missing := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: OpenLineageContract
metadata: {name: contract}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: OpenLineagePolicy
metadata: {name: policy}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata: {name: flow}
spec:
  receiverRef: {kind: HTTPReceiver, name: in}
  contractRef: {kind: OpenLineageContract, name: contract}
  policyRef: {kind: OpenLineagePolicy, name: policy}
  emitterRef: {kind: RedpandaEmitter, name: out}
  quarantineRef: {kind: QuarantineStore, name: quarantine}
`)
	_, err := Validate(context.Background(), NewCatalog(), missing)
	if !errors.Is(err, ErrMissingReference) {
		t.Fatalf("missing ref error = %v, want ErrMissingReference", err)
	}
	catalog := NewCatalog()
	if err := Register(catalog, Definition[testSpec]{GVK: GVK("Thing"), Capabilities: []Capability{CapabilityComponent}}); err != nil {
		t.Fatal(err)
	}
	mismatch := docsFromYAML(t, `apiVersion: eventflow.dev/v1alpha1
kind: Thing
metadata: {name: in}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: OpenLineageContract
metadata: {name: contract}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: OpenLineagePolicy
metadata: {name: policy}
spec: {}
---
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata: {name: flow}
spec:
  mode: standalone
  receiverRef: {kind: Thing, name: in}
  contractRef: {kind: OpenLineageContract, name: contract}
  policyRef: {kind: OpenLineagePolicy, name: policy}
  quarantineRef: {kind: Thing, name: in}
`)
	_, err = Validate(context.Background(), catalog, mismatch)
	if !errors.Is(err, ErrCapabilityMismatch) {
		t.Fatalf("capability error = %v, want ErrCapabilityMismatch", err)
	}
}

func TestResourceKindAllowed(t *testing.T) {
	if !ResourceKindAllowed("OpenLineagePolicy") {
		t.Fatal("OpenLineagePolicy should be allowed")
	}
	if ResourceKindAllowed("NotAResource") {
		t.Fatal("unexpected resource kind should not be allowed")
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
