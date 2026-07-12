# Architecture

Eventflow is a schema-enforcing CloudEvents gateway. It owns the boundary between producers and shared event infrastructure.

```text
producer
  -> receiver
  -> decode
  -> CloudEvents + payload contract validation
  -> deterministic metadata normalization
  -> durable journal
  -> dispatcher
  -> emitter
```

## Components

- Receivers accept HTTP, Kafka/Redpanda or filesystem input.
- Contracts validate CloudEvents envelope fields and optional JSON Schema payloads.
- The SQLite journal stores accepted events before acknowledgement.
- The dispatcher reads pending journal rows and delivers each record to each destination with bounded retry.
- Quarantine emitters receive invalid events when explicitly configured.
- Observability exposes health, readiness, metrics and structured operational fields.

## Trust Boundary

Eventflow is the governed admission boundary. Producers are not trusted to publish directly into shared infrastructure unless the platform team explicitly allows it. Eventflow validates what producers supplied; it does not infer business semantics.

## Deployment Modes

- Gateway: receiver plus journal append, with optional local dispatcher.
- Delivery worker: journal plus dispatcher and destinations.
- Replay job: journal query plus selected destination.
- Validate job: manifest validation only.

SQLite mode is single-writer/single-instance. Do not run multiple gateway replicas against one SQLite volume.

## Non-Goals

Eventflow is not a workflow engine, stream processor, service mesh, Dapr replacement, schema registry, data catalog, Kubernetes operator, broker administrator or infrastructure provisioner.
