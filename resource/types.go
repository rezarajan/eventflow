// Package resource loads and compiles OpenLineage admission gateway resources.
package resource

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

const APIVersion = "eventflow.dev/v1alpha1"

var (
	ErrInvalidDocument    = errors.New("invalid resource document")
	ErrDuplicateResource  = errors.New("duplicate resource")
	ErrUnknownKind        = errors.New("unknown resource kind")
	ErrMissingReference   = errors.New("missing resource reference")
	ErrCycle              = errors.New("resource dependency cycle")
	ErrCapabilityMismatch = errors.New("resource capability mismatch")
	ErrValidation         = errors.New("resource validation failed")
)

type Error struct {
	Kind error
	Path string
	Err  error
}

func (e Error) Error() string {
	if e.Path == "" {
		return e.Err.Error()
	}
	return e.Path + ": " + e.Err.Error()
}
func (e Error) Unwrap() error        { return e.Err }
func (e Error) Is(target error) bool { return target == e.Kind }

func typed(kind error, path string, err error) error {
	if err == nil {
		return nil
	}
	return Error{Kind: kind, Path: path, Err: err}
}

type GroupVersionKind struct {
	Group   string
	Version string
	Kind    string
}

func GVK(kind string) GroupVersionKind {
	return GroupVersionKind{Group: "eventflow.dev", Version: "v1alpha1", Kind: strings.TrimSpace(kind)}
}

func (g GroupVersionKind) String() string {
	return g.Group + "/" + g.Version + ", Kind=" + g.Kind
}

type ObjectMeta struct {
	Name        string            `yaml:"name" json:"name"`
	Labels      map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Annotations map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`
}

type Reference struct {
	Kind       string     `yaml:"kind" json:"kind"`
	Name       string     `yaml:"name" json:"name"`
	Capability Capability `yaml:"-" json:"-"`
}

func (r Reference) Key() ResourceKey {
	return ResourceKey{GVK: GVK(r.Kind), Name: strings.TrimSpace(r.Name)}
}

type Document struct {
	APIVersion string
	Kind       string
	Metadata   ObjectMeta
	Spec       *yaml.Node
	Source     string
	Index      int
}

func (d Document) GVK() GroupVersionKind { return GVK(d.Kind) }
func (d Document) Key() ResourceKey {
	return ResourceKey{GVK: d.GVK(), Name: strings.TrimSpace(d.Metadata.Name)}
}

type ResourceKey struct {
	GVK  GroupVersionKind
	Name string
}

func (k ResourceKey) String() string { return k.GVK.Kind + "/" + k.Name }

type Capability string

const (
	CapabilityComponent           Capability = "component"
	CapabilityOpenLineageContract Capability = "openLineageContract"
	CapabilityOpenLineagePolicy   Capability = "openLineagePolicy"
	CapabilityHTTPReceiver        Capability = "httpReceiver"
	CapabilityRedpandaEmitter     Capability = "redpandaEmitter"
	CapabilityRedpandaReceiver    Capability = "redpandaReceiver"
	CapabilityHTTPEmitter         Capability = "httpEmitter"
	CapabilityQuarantineStore     Capability = "quarantineStore"
	CapabilitySQLiteSpool         Capability = "sqliteSpool"
	CapabilityEventFlow           Capability = "eventFlow"
)

type Definition[T any] struct {
	GVK          GroupVersionKind
	Decode       func(*yaml.Node) (T, error)
	Default      func(*T) error
	Validate     func(context.Context, T) error
	References   func(T) []Reference
	Build        func(context.Context, BuildContext, T) (any, error)
	Capabilities []Capability
}

type definition interface {
	decode(*yaml.Node) (any, error)
	defaultSpec(any) (any, error)
	validate(context.Context, any) error
	references(any) []Reference
	build(context.Context, BuildContext, any) (any, error)
	capabilities() []Capability
}

type typedDefinition[T any] struct{ Definition[T] }

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

type Catalog struct {
	definitions map[string]definition
}

func NewCatalog() *Catalog {
	c := &Catalog{definitions: map[string]definition{}}
	RegisterCore(c)
	return c
}

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
