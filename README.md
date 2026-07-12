# Eventflow

Eventflow is an OpenLineage admission and quarantine gateway for shared data-platform infrastructure.

Use Eventflow when multiple independently operated systems emit OpenLineage into shared Kafka-compatible or metadata infrastructure where schema validity alone is insufficient. It validates producer identity, CloudEvents structure, OpenLineage payloads, and organization-defined lineage policies before events enter shared Redpanda, Kafka, or Marquez-backed paths.

Eventflow enforces:

- authenticated principal to job and dataset namespace authorization;
- supported OpenLineage schema versions and producer identifiers;
- required and prohibited facets;
- dataset URI scheme and naming rules;
- bounded event and facet size limits;
- stable rejection reason codes and operator-controlled quarantine.

Schema Registry remains complementary. It is better for schema storage and compatibility management. Eventflow is for admission decisions that combine identity, OpenLineage structure, and organization policy before a lineage event enters shared infrastructure.

Eventflow is unnecessary when one trusted producer writes directly to one reliable OpenLineage backend, when Schema Registry compatibility checks are the only requirement, or when the problem is general stream processing, arbitrary integration, workflow orchestration, service discovery, or lineage visualization.

## Quickstart

Run the automated admission demonstration:

```bash
just demo
```

The demo starts Redpanda, creates the OpenLineage topic, runs Eventflow, accepts an authorized valid event, rejects an unauthorized job namespace with `EF1202_JOB_NAMESPACE_NOT_ALLOWED`, rejects malformed OpenLineage with `EF1101_OPENLINEAGE_SCHEMA_INVALID`, lists quarantine records, validates a quarantined record, replays a corrected record, and shuts down the Eventflow process.

## Commands

```bash
eventflow validate --config examples/openlineage-redpanda-marquez/ingress.yaml
eventflow inspect --config examples/openlineage-redpanda-marquez/ingress.yaml
eventflow run --config examples/openlineage-redpanda-marquez/ingress.yaml
eventflow policy test --config examples/openlineage-redpanda-marquez/ingress.yaml --event event.json --principal spiffe://example/dagster
eventflow quarantine list --config examples/openlineage-redpanda-marquez/ingress.yaml
eventflow status --config examples/openlineage-redpanda-marquez/ingress.yaml
```

Supported quarantine commands are `list`, `show <id>`, `validate <id>`, `replay <id>`, and `dismiss <id>`.

## Resource Kinds

The active resource model is deliberately small:

- `OpenLineageContract`
- `OpenLineagePolicy`
- `HTTPReceiver`
- `RedpandaEmitter`
- `RedpandaReceiver`
- `HTTPEmitter`
- `QuarantineStore`
- `SQLiteSpool`
- `EventFlow`

No adapter marketplace, observation graph, transformation DSL, Kubernetes operator, Schema Registry replacement, or Marquez replacement is included.

## Deployment Modes

HTTP-to-Kafka:

```text
HTTP -> CloudEvents/OpenLineage/policy validation -> Redpanda/Kafka
```

The HTTP request is acknowledged only after the broker acknowledges publication. By default broker outages are rejected and quarantined with `EF1501_BROKER_UNAVAILABLE`. Optional SQLite spooling must be explicitly configured and is single-instance only.

Kafka-to-Marquez:

```text
Redpanda/Kafka -> policy validation -> Marquez HTTP
```

Kafka remains the durable source of truth. Source offsets are committed only after successful Marquez delivery. Terminal policy failures are quarantined.

Standalone spool:

```text
HTTP -> SQLite spool -> HTTP destination
```

This is a single-instance mode for small deployments and incident workflows, not a highly available broker substitute.

See `docs/comparison.md`, `docs/policies.md`, `docs/quarantine.md`, and `docs/deployment/` for operational details.
