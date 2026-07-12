package resource

import (
	"context"
	"fmt"
	"sort"
)

type Graph struct {
	nodes map[ResourceKey]*node
	order []ResourceKey
}

func (g *Graph) Nodes() []ResourceKey {
	if g == nil {
		return nil
	}
	keys := make([]ResourceKey, 0, len(g.nodes))
	for key := range g.nodes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	return keys
}

type node struct {
	doc          Document
	def          definition
	spec         any
	dependencies []Reference
}

type BuildContext struct {
	objects map[ResourceKey]any
	current ResourceKey
}

func (b BuildContext) ResourceName() string { return b.current.Name }

func (b BuildContext) Get(ref Reference) (any, error) {
	obj, ok := b.objects[ref.Key()]
	if !ok {
		return nil, typed(ErrMissingReference, ref.Key().String(), fmt.Errorf("resource is not built"))
	}
	return obj, nil
}

type Compiled struct {
	Objects map[ResourceKey]any
	Flows   []Flow
}

func Validate(ctx context.Context, catalog *Catalog, docs []Document) (*Graph, error) {
	graph := &Graph{nodes: map[ResourceKey]*node{}}
	for _, doc := range docs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := doc.Key()
		if _, exists := graph.nodes[key]; exists {
			return nil, typed(ErrDuplicateResource, key.String(), fmt.Errorf("duplicate resource identity"))
		}
		def, ok := catalog.definition(doc.GVK())
		if !ok {
			return nil, typed(ErrUnknownKind, key.String(), fmt.Errorf("kind %q is not registered", doc.Kind))
		}
		spec, err := def.decode(doc.Spec)
		if err != nil {
			return nil, typed(ErrValidation, key.String(), fmt.Errorf("decode spec: %w", err))
		}
		spec, err = def.defaultSpec(spec)
		if err != nil {
			return nil, typed(ErrValidation, key.String(), fmt.Errorf("default spec: %w", err))
		}
		if err := def.validate(ctx, spec); err != nil {
			return nil, typed(ErrValidation, key.String(), err)
		}
		graph.nodes[key] = &node{doc: doc, def: def, spec: spec, dependencies: def.references(spec)}
	}
	for key, node := range graph.nodes {
		for _, ref := range node.dependencies {
			dep, ok := graph.nodes[ref.Key()]
			if !ok {
				return nil, typed(ErrMissingReference, key.String(), fmt.Errorf("missing %s", ref.Key().String()))
			}
			if ref.Capability != "" && !hasCapability(dep.def.capabilities(), ref.Capability) {
				return nil, typed(ErrCapabilityMismatch, key.String(), fmt.Errorf("%s does not declare %s", ref.Key().String(), ref.Capability))
			}
		}
	}
	order, err := topo(graph)
	if err != nil {
		return nil, err
	}
	graph.order = order
	return graph, nil
}

func Compile(ctx context.Context, catalog *Catalog, docs []Document) (*Compiled, error) {
	graph, err := Validate(ctx, catalog, docs)
	if err != nil {
		return nil, err
	}
	compiled := &Compiled{Objects: map[ResourceKey]any{}}
	bctx := BuildContext{objects: compiled.Objects}
	for _, key := range graph.order {
		n := graph.nodes[key]
		bctx.current = key
		obj, err := n.def.build(ctx, bctx, n.spec)
		if err != nil {
			return nil, typed(ErrValidation, key.String(), fmt.Errorf("build: %w", err))
		}
		compiled.Objects[key] = obj
		if flow, ok := obj.(Flow); ok {
			compiled.Flows = append(compiled.Flows, flow)
		}
	}
	sort.Slice(compiled.Flows, func(i, j int) bool { return compiled.Flows[i].Name < compiled.Flows[j].Name })
	return compiled, nil
}

func topo(graph *Graph) ([]ResourceKey, error) {
	state := map[ResourceKey]int{}
	var order []ResourceKey
	keys := make([]ResourceKey, 0, len(graph.nodes))
	for key := range graph.nodes {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].String() < keys[j].String() })
	var visit func(ResourceKey) error
	visit = func(key ResourceKey) error {
		switch state[key] {
		case 1:
			return typed(ErrCycle, key.String(), fmt.Errorf("cycle includes %s", key.String()))
		case 2:
			return nil
		}
		state[key] = 1
		for _, ref := range graph.nodes[key].dependencies {
			if err := visit(ref.Key()); err != nil {
				return err
			}
		}
		state[key] = 2
		order = append(order, key)
		return nil
	}
	for _, key := range keys {
		if err := visit(key); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func hasCapability(caps []Capability, want Capability) bool {
	for _, cap := range caps {
		if cap == want {
			return true
		}
	}
	return false
}
