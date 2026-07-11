# Agent Notes

Eventflow is a domain-neutral Go SDK plus a declarative resource runtime.
Keep runtime code free of sample event types, sample schemas, and sample
projection tables.

## Architecture Rules

- Domain knowledge comes from explicit `eventflow.dev/v1alpha1` resources.
- CloudEvents is the in-memory and wire event envelope.
- JSON Schema references belong to `EventContract` resources or domain tooling.
- OpenLineage is operational metadata and must not be embedded into domain event payloads.
- Public packages must remain importable; avoid putting new SDK behavior only under `internal`.
- Adapters register resource definitions explicitly; do not add global registries or `init()` registration.
- Do not add Datascape control-plane, provisioning, identity, governance, or resource ownership workflows.

## Core Commands

```bash
go run ./cmd/eventflow validate --config examples/school/eventflow.yaml
go run ./cmd/eventflow inspect --config examples/school/eventflow.yaml
go run ./cmd/eventflow run --config examples/school/eventflow.yaml
go run ./cmd/eventflow-emit
go run ./cmd/eventflow-receive
go run ./cmd/eventflow-relay
go run ./cmd/eventflow-lineage-replay
```

## Testing

Use:

```bash
GOCACHE=/tmp/eventflow-go-build-cache go test ./...
GOCACHE=/tmp/eventflow-go-build-cache go test -race ./...
GOCACHE=/tmp/eventflow-go-build-cache go vet ./...
```
