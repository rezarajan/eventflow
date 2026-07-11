// Package resource loads, validates, and compiles Eventflow declarative resources.
package resource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"

	eventflow "github.com/rezarajan/eventflow"
	"gopkg.in/yaml.v3"
)

// APIVersion is the only declarative resource API version supported by this package.
const APIVersion = "eventflow.dev/v1alpha1"

var (
	// ErrInvalidDocument marks malformed YAML or invalid resource envelopes.
	ErrInvalidDocument = errors.New("invalid resource document")
	// ErrDuplicateResource marks repeated kind/name identities in one compile set.
	ErrDuplicateResource = errors.New("duplicate resource")
	// ErrUnknownKind marks a resource kind that is not registered in the Catalog.
	ErrUnknownKind = errors.New("unknown resource kind")
	// ErrMissingReference marks a reference to a resource that was not loaded.
	ErrMissingReference = errors.New("missing resource reference")
	// ErrCycle marks a circular dependency between resources.
	ErrCycle = errors.New("resource dependency cycle")
	// ErrCapabilityMismatch marks a reference to a resource with the wrong capability.
	ErrCapabilityMismatch = errors.New("resource capability mismatch")
	// ErrInvalidValueSource marks malformed literal/env/file value source configuration.
	ErrInvalidValueSource = errors.New("invalid value source")
	// ErrValidation marks spec decoding, defaulting, validation, or build failures.
	ErrValidation = errors.New("resource validation failed")
)

// Error wraps resource compiler failures with a stable error kind and resource path.
type Error struct {
	// Kind is one of the stable Err* sentinels exported by this package.
	Kind error
	// Path identifies the resource or document location associated with Err.
	Path string
	// Err is the concrete underlying failure.
	Err error
}

// Error returns a human-readable diagnostic.
func (e Error) Error() string {
	if e.Path == "" {
		return e.Err.Error()
	}
	return e.Path + ": " + e.Err.Error()
}

// Unwrap returns the concrete underlying failure.
func (e Error) Unwrap() error { return e.Err }

// Is reports whether target matches the stable error kind.
func (e Error) Is(target error) bool { return target == e.Kind }

func typed(kind error, path string, err error) error {
	if err == nil {
		return nil
	}
	return Error{Kind: kind, Path: path, Err: err}
}

// GroupVersionKind identifies a resource definition independently from a name.
type GroupVersionKind struct {
	// Group is the API group, for example eventflow.dev.
	Group string
	// Version is the API version, for example v1alpha1.
	Version string
	// Kind is the resource kind, for example FilesystemEmitter.
	Kind string
}

// GVK returns the Eventflow v1alpha1 group/version/kind for kind.
func GVK(kind string) GroupVersionKind {
	return GroupVersionKind{Group: "eventflow.dev", Version: "v1alpha1", Kind: strings.TrimSpace(kind)}
}

func (g GroupVersionKind) String() string {
	if g.Group == "" && g.Version == "" {
		return g.Kind
	}
	return g.Group + "/" + g.Version + ", Kind=" + g.Kind
}

// ObjectMeta carries resource identity and optional descriptive metadata.
type ObjectMeta struct {
	// Name is the unique resource name within its kind.
	Name string `yaml:"name" json:"name"`
	// Labels are user-defined key/value pairs for organization.
	Labels map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	// Annotations are user-defined descriptive metadata.
	Annotations map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`
}

// Reference names another resource and, internally, the capability required from it.
type Reference struct {
	// Kind is the referenced resource kind.
	Kind string `yaml:"kind" json:"kind"`
	// Name is the referenced resource metadata.name.
	Name string `yaml:"name" json:"name"`
	// Capability is set by resource definitions during validation.
	Capability Capability `yaml:"-" json:"-"`
}

// Key returns the stable resource identity targeted by r.
func (r Reference) Key() ResourceKey {
	return ResourceKey{GVK: GVK(r.Kind), Name: strings.TrimSpace(r.Name)}
}

// Document is a parsed YAML resource envelope with an undecoded spec node.
type Document struct {
	// APIVersion is the declarative API version from the YAML envelope.
	APIVersion string
	// Kind is the resource kind from the YAML envelope.
	Kind string
	// Metadata carries resource identity and descriptive metadata.
	Metadata ObjectMeta
	// Spec is the raw YAML spec node decoded by the registered Definition.
	Spec *yaml.Node
	// Source is the filename or synthetic source label used in diagnostics.
	Source string
	// Index is the zero-based document index within Source.
	Index int
}

// GVK returns the document's Eventflow group/version/kind.
func (d Document) GVK() GroupVersionKind { return GVK(d.Kind) }

// Key returns the document's kind/name identity.
func (d Document) Key() ResourceKey {
	return ResourceKey{GVK: d.GVK(), Name: strings.TrimSpace(d.Metadata.Name)}
}

// ResourceKey is the unique identity of a loaded resource.
type ResourceKey struct {
	// GVK identifies the registered resource definition.
	GVK GroupVersionKind
	// Name is metadata.name.
	Name string
}

// String returns a compact kind/name representation suitable for diagnostics.
func (k ResourceKey) String() string { return k.GVK.Kind + "/" + k.Name }

// Capability describes what a resource definition can provide to other resources.
type Capability string

const (
	// CapabilityComponent marks any buildable resource component.
	CapabilityComponent Capability = "component"
	// CapabilityEmitter marks resources that implement eventflow.Emitter.
	CapabilityEmitter Capability = "emitter"
	// CapabilityReceiver marks resources that implement eventflow.Receiver.
	CapabilityReceiver Capability = "receiver"
	// CapabilityObserver marks resources that implement eventflow.Observer.
	CapabilityObserver Capability = "observer"
	// CapabilityObservationMapper marks resources that implement eventflow.ObservationMapper.
	CapabilityObservationMapper Capability = "observationMapper"
	// CapabilityObservationSource marks adapter-specific observation input sources.
	CapabilityObservationSource Capability = "observationSource"
	// CapabilityValidator marks resources that implement eventflow.Validator.
	CapabilityValidator Capability = "validator"
	// CapabilityCodec marks resources that implement eventflow.Codec.
	CapabilityCodec Capability = "codec"
	// CapabilityBatchEmission marks emitters that also implement eventflow.BatchEmitter.
	CapabilityBatchEmission Capability = "batchEmission"
	// CapabilityEventContract marks EventContract resources.
	CapabilityEventContract Capability = "eventContract"
	// CapabilityEventFlow marks compiled EventFlow resources.
	CapabilityEventFlow Capability = "eventFlow"
	// CapabilityObservationFlow marks compiled ObservationFlow resources.
	CapabilityObservationFlow Capability = "observationFlow"
)

// Component is implemented by named adapter components.
type Component interface{ Name() string }

// EmitsEvents is implemented by resources that can emit CloudEvents.
type EmitsEvents interface{ eventflow.Emitter }

// ReceivesEvents is implemented by resources that can receive CloudEvents.
type ReceivesEvents interface{ eventflow.Receiver }

// ObservesActivity is implemented by resources that can observe platform activity.
type ObservesActivity interface{ eventflow.Observer }

// MapsObservations is implemented by resources that can convert observations into CloudEvents.
type MapsObservations interface{ eventflow.ObservationMapper }

// ValidatesEvents is implemented by resources that can validate CloudEvents.
type ValidatesEvents interface{ eventflow.Validator }

// EncodesEvents is implemented by resources that can encode and decode CloudEvents.
type EncodesEvents interface{ eventflow.Codec }

// SupportsBatchEmission is implemented by emitters that can emit batches.
type SupportsBatchEmission interface{ eventflow.BatchEmitter }

// Literal represents a literal typed value in a value source.
type Literal[T any] struct {
	Value T `yaml:"value" json:"value"`
}

// Env represents a typed value read from an environment variable.
type Env[T any] struct {
	Name    string `yaml:"name" json:"name"`
	Default T      `yaml:"default,omitempty" json:"default,omitempty"`
}

// File represents a typed value read from a local file.
type File[T any] struct {
	Path string `yaml:"path" json:"path"`
}

// ValueSource describes one literal, environment, or file-backed value.
//
// ValueSource intentionally does not implement string interpolation. Resource
// definitions should resolve only the fields where indirect values are safe.
type ValueSource[T any] struct {
	Value *T       `yaml:"value,omitempty" json:"value,omitempty"`
	Env   *Env[T]  `yaml:"env,omitempty" json:"env,omitempty"`
	File  *File[T] `yaml:"file,omitempty" json:"file,omitempty"`
}

// Resolve returns the typed value represented by v.
//
// The parse function is used for environment and file values. Exactly one of
// value, env, or file must be set.
func (v ValueSource[T]) Resolve(parse func(string) (T, error)) (T, error) {
	var zero T
	count := 0
	if v.Value != nil {
		count++
	}
	if v.Env != nil {
		count++
	}
	if v.File != nil {
		count++
	}
	if count == 0 {
		return zero, typed(ErrInvalidValueSource, "", fmt.Errorf("value source must set one of value, env, or file"))
	}
	if count > 1 {
		return zero, typed(ErrInvalidValueSource, "", fmt.Errorf("value source must not combine value, env, and file"))
	}
	if v.Value != nil {
		return *v.Value, nil
	}
	if parse == nil {
		return zero, typed(ErrInvalidValueSource, "", fmt.Errorf("parser is required"))
	}
	if v.Env != nil {
		if strings.TrimSpace(v.Env.Name) == "" {
			return zero, typed(ErrInvalidValueSource, "", fmt.Errorf("env.name is required"))
		}
		if raw, ok := os.LookupEnv(v.Env.Name); ok {
			return parse(raw)
		}
		return v.Env.Default, nil
	}
	body, err := os.ReadFile(v.File.Path)
	if err != nil {
		return zero, err
	}
	return parse(strings.TrimSpace(string(body)))
}

// Definition describes one registered resource kind.
//
// T is the strict spec type decoded from the resource's spec node. Definitions
// own defaulting, semantic validation, dependency discovery, build behavior,
// and declared capabilities for their kind.
type Definition[T any] struct {
	// GVK is the resource identity handled by this definition.
	GVK GroupVersionKind
	// Schema is reserved for JSON Schema metadata exposed by tooling.
	Schema any
	// Decode converts a raw YAML spec node into T. Nil uses DecodeStrict[T].
	Decode func(*yaml.Node) (T, error)
	// Default mutates a decoded spec before semantic validation.
	Default func(*T) error
	// Validate performs kind-specific semantic validation.
	Validate func(context.Context, T) error
	// References returns the resources this spec depends on.
	References func(T) []Reference
	// Build constructs the runtime object for this spec.
	Build func(context.Context, BuildContext, T) (any, error)
	// Capabilities declares what the built object can provide to references.
	Capabilities []Capability
}

type definition interface {
	gvk() GroupVersionKind
	decode(*yaml.Node) (any, error)
	defaultSpec(any) (any, error)
	validate(context.Context, any) error
	references(any) []Reference
	build(context.Context, BuildContext, any) (any, error)
	capabilities() []Capability
}

type typedDefinition[T any] struct{ Definition[T] }

func (d typedDefinition[T]) gvk() GroupVersionKind { return d.GVK }
func (d typedDefinition[T]) decode(node *yaml.Node) (any, error) {
	if d.Decode != nil {
		return d.Decode(node)
	}
	return DecodeStrict[T](node)
}
func (d typedDefinition[T]) defaultSpec(v any) (any, error) {
	spec := v.(T)
	if d.Default != nil {
		if err := d.Default(&spec); err != nil {
			return nil, err
		}
	}
	return spec, nil
}
func (d typedDefinition[T]) validate(ctx context.Context, v any) error {
	if d.Validate == nil {
		return nil
	}
	return d.Validate(ctx, v.(T))
}
func (d typedDefinition[T]) references(v any) []Reference {
	if d.References == nil {
		return nil
	}
	return d.References(v.(T))
}
func (d typedDefinition[T]) build(ctx context.Context, bctx BuildContext, v any) (any, error) {
	if d.Build == nil {
		return v, nil
	}
	return d.Build(ctx, bctx, v.(T))
}
func (d typedDefinition[T]) capabilities() []Capability {
	return append([]Capability(nil), d.Capabilities...)
}

// Catalog stores explicitly registered resource definitions.
//
// Catalogs are caller-owned. Eventflow does not use package-level registration
// or init-time side effects.
type Catalog struct {
	definitions map[string]definition
}

// NewCatalog returns a Catalog with core EventContract, EventFlow, and ObservationFlow definitions.
func NewCatalog() *Catalog {
	c := &Catalog{definitions: map[string]definition{}}
	RegisterCore(c)
	return c
}

// Register adds def to catalog.
//
// Register fails when catalog is nil, the definition kind is empty, or the same
// group/version/kind has already been registered.
func Register[T any](catalog *Catalog, def Definition[T]) error {
	if catalog == nil {
		return fmt.Errorf("catalog is required")
	}
	if strings.TrimSpace(def.GVK.Kind) == "" {
		return fmt.Errorf("definition kind is required")
	}
	key := def.GVK.String()
	if _, exists := catalog.definitions[key]; exists {
		return fmt.Errorf("definition %s already registered", key)
	}
	catalog.definitions[key] = typedDefinition[T]{Definition: def}
	return nil
}

func (c *Catalog) definition(gvk GroupVersionKind) (definition, bool) {
	if c == nil {
		return nil, false
	}
	def, ok := c.definitions[gvk.String()]
	return def, ok
}

// DecodeStrict decodes node into T after rejecting unknown YAML fields.
//
// Resource definitions use DecodeStrict by default. Supplying a custom Decode
// function is appropriate when a spec needs union decoding or custom scalar
// parsing that cannot be expressed with struct tags alone.
func DecodeStrict[T any](node *yaml.Node) (T, error) {
	var out T
	if node == nil {
		return out, nil
	}
	if err := rejectUnknownFields(node, reflect.TypeOf(out)); err != nil {
		return out, err
	}
	if err := node.Decode(&out); err != nil {
		return out, err
	}
	return out, nil
}

func rejectUnknownFields(node *yaml.Node, typ reflect.Type) error {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct || node.Kind != yaml.MappingNode {
		return nil
	}
	fields := yamlFields(typ)
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		field, ok := fields[key]
		if !ok {
			return fmt.Errorf("unknown field %q", key)
		}
		if err := rejectUnknownFields(node.Content[i+1], field); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
	}
	return nil
}

func yamlFields(typ reflect.Type) map[string]reflect.Type {
	fields := map[string]reflect.Type{}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		if field.Anonymous {
			for key, value := range yamlFields(field.Type) {
				fields[key] = value
			}
			continue
		}
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "-" {
			continue
		}
		if name == "" {
			name = field.Name
		}
		fields[name] = field.Type
	}
	return fields
}
