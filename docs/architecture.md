# Architecture

Eventflow is an OpenLineage admission and quarantine gateway.

```text
producer
  -> HTTP or Kafka receiver
  -> authenticated principal extraction
  -> CloudEvents validation
  -> OpenLineage shape and version validation
  -> typed organization policy
  -> accepted destination or quarantine
```

## Trust Boundary

Eventflow sits at the governed boundary before shared Kafka-compatible or metadata infrastructure. It does not generate lineage semantics; Dagster, Spark, dbt, and custom producers remain responsible for accurate OpenLineage jobs, runs, datasets, and facets.

## Components

- `OpenLineageContract` defines the supported CloudEvents type and OpenLineage schema versions.
- `OpenLineagePolicy` defines identity, namespace, facet, size, naming, tenant, environment, and rate-limit rules.
- `HTTPReceiver` accepts OpenLineage over HTTP and trusts identity only from verified transport or trusted proxy configuration.
- `RedpandaEmitter` publishes accepted events to an existing topic.
- `RedpandaReceiver` consumes from an existing topic for Kafka-to-Marquez delivery.
- `HTTPEmitter` delivers accepted events to Marquez-compatible HTTP endpoints.
- `QuarantineStore` persists rejected events and admission decisions for operator inspection and replay.
- `SQLiteSpool` is opt-in single-instance local spooling for standalone or explicit broker-outage handling.

## Deployment Modes

- HTTP-to-Kafka: broker acknowledgement is the durability boundary.
- Kafka-to-Marquez: Kafka offsets and retention are the durability boundary.
- Standalone spool: SQLite is local single-instance durability.

Eventflow does not provision topics, buckets, schemas, identities, or Kubernetes control planes.
