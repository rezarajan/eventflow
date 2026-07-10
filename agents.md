# Agent Notes

Eventflow is a domain-neutral Go SDK plus thin runtime commands. Keep runtime code free of sample event types, sample schemas, and sample projection tables.

## Architecture Rules

- Domain knowledge comes from `EVENTFLOW_REGISTRY`.
- CloudEvents is the domain event envelope.
- JSON Schema validates producer payloads.
- AsyncAPI is generated or checked from registry data.
- OpenLineage is operational metadata and must not be embedded into CloudEvents.
- Commands should stay small and Unix-like.
- Public packages must remain importable; avoid putting new SDK behavior only under `internal`.
- Strict registry-driven validation is the default.
- Do not add Datascape control-plane, provisioning, identity, governance, or resource ownership workflows.

## Core Commands

```bash
go run ./cmd/eventflow-registry validate --registry examples/school/eventflow.yaml
go run ./cmd/eventflow-emit
go run ./cmd/eventflow-receive
go run ./cmd/eventflow-relay
go run ./cmd/eventflow-ingress-http
go run ./cmd/eventflow-fanout
go run ./cmd/eventflow-consume
go run ./cmd/eventflow-lineage-replay
```

## Configuration

Use `EVENTFLOW_*` environment variables. `DATASCAPE_*` names are deprecated compatibility aliases only.

Important variables:

- `EVENTFLOW_REGISTRY`
- `EVENTFLOW_REDPANDA_BROKERS`
- `EVENTFLOW_REDPANDA_TOPIC`
- `EVENTFLOW_REDPANDA_TOPIC_MODE`
- `EVENTFLOW_CONSUME_HANDLERS`
- `EVENTFLOW_DUCKDB_PATH`
- `EVENTFLOW_LINEAGE_OUTPUT`
- `EVENTFLOW_LINEAGE_FILE`

## Examples

The school domain under `examples/school` is an example only. Do not reintroduce those event types or schemas into core runtime defaults.

## Testing

Use:

```bash
GOCACHE=/tmp/eventflow-go-build-cache go test ./...
GOCACHE=/tmp/eventflow-go-build-cache go test -race ./...
GOCACHE=/tmp/eventflow-go-build-cache go vet ./...
```
