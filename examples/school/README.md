# School Example

This example shows how a domain describes Eventflow resources without adding
domain-specific code to the SDK.

`eventflow.yaml` contains:

- filesystem input and output resources,
- `EventContract` resources for selected school events,
- an `EventFlow` that links the receiver, contracts, and emitter.

Validate the resource file from the repository root:

```bash
go run ./cmd/eventflow validate --config examples/school/eventflow.yaml
```

Inspect the compiled resource graph:

```bash
go run ./cmd/eventflow inspect --config examples/school/eventflow.yaml
```

To run the example, create an input file at `examples/school/var/school-events.ndjson`
containing structured CloudEvents that match the declared contract types, then run:

```bash
go run ./cmd/eventflow run --config examples/school/eventflow.yaml
```

Payload schemas and SQL DDL under `contracts/` are domain artifacts. Eventflow
does not compile these event types into core runtime defaults.
