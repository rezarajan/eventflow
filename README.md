# Eventflow

Eventflow is a schema-enforcing CloudEvents gateway for teams that operate shared event and lineage infrastructure.

It accepts events from systems such as Dagster, Spark, dbt, custom applications, and HTTP producers; validates them against versioned contracts; durably records accepted events; and delivers them to Kafka-compatible brokers or downstream services such as Marquez.

```text
Producer
   |
   v
Eventflow
   |-- validate the CloudEvents envelope
   |-- validate the payload contract
   |-- normalize configured technical metadata
   |-- durably journal accepted events
   |-- quarantine invalid events
   |-- retry failed destinations
   `-- replay selected records
   |
   v
Redpanda / Kafka / Marquez / HTTP destination
```

Deploy Eventflow when producers should not write directly into shared event or lineage infrastructure without validation and recoverable delivery.

## Why Eventflow?

Kafka, Redpanda, Schema Registry, Dapr, and OpenLineage clients already solve important parts of event-driven infrastructure. Eventflow does not replace them.

Eventflow provides the governed boundary around them.

| Component           | Primary responsibility                                                              |
| ------------------- | ----------------------------------------------------------------------------------- |
| Kafka or Redpanda   | Distributed event storage and transport                                             |
| Schema Registry     | Schema storage, evolution, and compatibility                                        |
| OpenLineage clients | Generation and emission of lineage events                                           |
| Marquez             | Lineage storage, querying, and visualization                                        |
| Dapr                | Application connectivity, service invocation, and Pub/Sub                           |
| Eventflow           | Event admission, contract enforcement, quarantine, recoverable delivery, and replay |

Schema Registry can determine whether a payload conforms to an approved schema. Eventflow additionally determines whether the complete event is permitted across a system boundary and records what happened after that decision.

Eventflow can enforce:

* CloudEvents envelope requirements;
* supported event types;
* versioned payload contracts;
* required extensions and technical metadata;
* configured source and subject constraints;
* invalid-event handling;
* durable acknowledgement boundaries;
* independent destination delivery state;
* operator-controlled replay.

## Do I Need Eventflow?

| Scenario                                                                                     | Use Eventflow? | Reason                                                               |
| -------------------------------------------------------------------------------------------- | -------------: | -------------------------------------------------------------------- |
| Dagster sends OpenLineage directly to a reliable Marquez instance                            |     Usually no | The direct integration is simpler                                    |
| Marquez outages must not interrupt Dagster or Spark runs                                     |            Yes | Eventflow journals accepted events and retries delivery              |
| Several teams publish into shared lineage infrastructure                                     |            Yes | Eventflow provides a common validation and quarantine boundary       |
| HTTP producers must publish into Redpanda without receiving broker credentials               |            Yes | Eventflow isolates producers from the broker                         |
| Kafka producers already use Schema Registry and consumers already handle failures and replay |     Usually no | Eventflow may duplicate existing controls                            |
| One Go service publishes to one Kafka topic                                                  |     Usually no | Use a Kafka client and the CloudEvents SDK directly                  |
| Complex joins, windows, filtering, or aggregation are required                               |             No | Use a stream processor                                               |
| A multi-step business transaction must be coordinated reliably                               |             No | Use Temporal or another workflow engine                              |
| A Dapr application needs governed lineage admission                                          |       Possibly | Dapr handles connectivity; Eventflow handles validation and recovery |

## Primary Use Case: OpenLineage

OpenLineage is Eventflow's primary built-in profile.

Producers such as Dagster, Spark, dbt, Airflow, and custom applications remain responsible for generating accurate lineage semantics:

* jobs;
* runs;
* datasets;
* facets;
* parent-child relationships;
* event lifecycle transitions.

Eventflow does not reconstruct or infer those semantics. It validates, journals, transports, quarantines, retries, and replays the events supplied by the producer.

A typical deployment is:

```text
Dagster / Spark / dbt
          |
          | OpenLineage over HTTP
          v
Eventflow ingress
          |
          | validated CloudEvents
          v
Kafka / Redpanda
          |
          v
Eventflow delivery worker
          |
          | OpenLineage HTTP
          v
Marquez
```

This separates producer availability from downstream availability and prevents malformed events from entering shared lineage infrastructure.

## Core Capabilities

### Contract enforcement

`EventContract` resources define the CloudEvents type and optional payload schema accepted by a flow.

Eventflow validates:

* required CloudEvents attributes;
* event type;
* configured source and subject rules;
* required extensions;
* payload schemas;
* references between resources.

### Strict configuration compilation

Eventflow resources use the `eventflow.dev/v1alpha1` API.

Before a flow runs, the compiler rejects:

* unknown resource envelope fields;
* unknown specification fields;
* duplicate resource identities;
* missing references;
* invalid contract references;
* dependency cycles;
* adapter capability mismatches.

Configuration errors fail at startup rather than appearing later as partially initialized runtime failures.

### Durable admission

Accepted events are acknowledged only after they have been:

1. decoded;
2. matched to a contract;
3. validated;
4. durably appended to the configured journal.

Destination delivery occurs after this boundary.

### At-least-once delivery

Eventflow provides at-least-once delivery after durable journal append.

A destination may receive an event more than once after a timeout, process restart, or ambiguous network failure. Downstream services should therefore use the CloudEvents event ID or another stable identifier for idempotency.

Eventflow does not claim exactly-once delivery.

### Per-destination state

When a flow has multiple emitters, Eventflow tracks delivery independently for each destination.

One failed destination does not erase a successful delivery to another destination.

### Quarantine

Invalid events can be routed away from normal destinations with their validation failure preserved for inspection.

Quarantined events are never silently treated as accepted business events.

### Replay

Failed or selected journal records can be replayed without requiring the original producer to emit them again.

By default, replay preserves the original CloudEvents identity.

## Smallest Useful Deployment

The smallest production-shaped deployment consists of one `eventflow run` process with:

* one ingress receiver;
* at least one `EventContract`;
* one `EventFlow`;
* one `SQLiteJournal`;
* one destination emitter;
* one invalid-event emitter for quarantine.

```text
HTTPReceiver
     |
     v
EventContract
     |
     v
SQLiteJournal
     |
     v
RedpandaEmitter
```

SQLite provides durable state for a single Eventflow instance. It is not a distributed or horizontally scalable journal. Deployments requiring multiple active writers need a different coordination model.

## Resource Model

Every resource uses a common envelope:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: openlineage-run-event
spec:
  type: io.openlineage.run-event.v1
  dataSchema: ./schemas/openlineage-run-event.json
```

An `EventFlow` connects a receiver, contracts, journal, destinations, and invalid-event route:

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventFlow
metadata:
  name: lineage-ingress
spec:
  receiverRef:
    kind: HTTPReceiver
    name: lineage-http

  contractRefs:
    - kind: EventContract
      name: openlineage-run-event

  journalRef:
    kind: SQLiteJournal
    name: gateway-journal

  emitterRefs:
    - kind: RedpandaEmitter
      name: lineage-events

  invalidEmitterRef:
    kind: FilesystemEmitter
    name: quarantine
```

The exact receiver, journal, and emitter resources are declared separately in the same configuration set.

Eventflow connects to existing infrastructure. It does not create Kafka topics, Redpanda clusters, buckets, databases, or downstream services.

## Supported Runtime Roles

### HTTP ingress

Accept CloudEvents or supported OpenLineage requests over HTTP, validate them, journal accepted events, and dispatch them to configured destinations.

### Kafka-compatible ingress and egress

Consume from or publish to existing Kafka and Redpanda topics.

Eventflow is responsible for event-level validation and dispatch behavior, not broker provisioning or administration.

### HTTP delivery

Deliver accepted events to downstream HTTP services such as Marquez.

### Filesystem integration

Filesystem receivers and emitters are intended primarily for:

* development;
* fixtures;
* incident recovery;
* import and export;
* local testing;
* quarantine inspection.

They are not the primary production transport.

## Quickstart: OpenLineage to Redpanda and Marquez

### 1. Start the local infrastructure

```bash
just up-all
just topic openlineage.events.v1
```

### 2. Validate the ingress configuration

```bash
go run ./cmd/eventflow validate \
  --config examples/openlineage-redpanda-marquez/ingress.yaml
```

Validation checks resource decoding, references, contracts, cycles, and adapter capabilities without starting the runtime.

### 3. Inspect the compiled flow

```bash
go run ./cmd/eventflow inspect \
  --config examples/openlineage-redpanda-marquez/ingress.yaml
```

Inspection prints the compiled receiver, contracts, journal, and destinations.

### 4. Start the ingress gateway

```bash
go run ./cmd/eventflow run \
  --config examples/openlineage-redpanda-marquez/ingress.yaml
```

### 5. Submit a valid OpenLineage event

```bash
curl -fsS -X POST http://localhost:8080/events \
  -H 'content-type: application/json' \
  --data @examples/openlineage-redpanda-marquez/valid-run-event.json
```

The request is acknowledged only after validation and durable journal append.

### 6. Start the delivery worker

```bash
go run ./cmd/eventflow run \
  --config examples/openlineage-redpanda-marquez/worker.yaml
```

The worker consumes accepted events and delivers them to Marquez.

### 7. Replay failed deliveries

After the destination becomes available again:

```bash
go run ./cmd/eventflow replay \
  --config examples/openlineage-redpanda-marquez/worker.yaml \
  --destination HTTPEmitter/marquez \
  --state FAILED
```

## Commands

Eventflow exposes one primary command with focused subcommands:

```text
eventflow validate
eventflow inspect
eventflow run
eventflow replay
```

### Validate configuration

```bash
eventflow validate --config resources.yaml
```

Loads and compiles resources without starting any receivers or workers.

Use this command in CI to prevent invalid gateway configuration from being deployed.

### Inspect a flow

```bash
eventflow inspect --config resources.yaml
```

Displays the compiled resource graph and resolved runtime components.

### Run Eventflow

```bash
eventflow run --config resources.yaml
```

Starts the configured receiver, journal, dispatcher, and emitters.

### Replay records

```bash
eventflow replay \
  --config resources.yaml \
  --destination HTTPEmitter/marquez \
  --state FAILED
```

Replays matching records to the selected destination.

## Delivery Semantics

The supported delivery guarantee is:

> At-least-once delivery after durable journal append.

The practical consequences are:

* an HTTP request is not accepted before journal persistence succeeds;
* a journal failure causes admission to fail;
* delivery failures are recorded per destination;
* pending records survive a process restart;
* retries use bounded behavior;
* replay may produce duplicate delivery;
* graceful shutdown stops new admission and drains in-flight work within the configured limit.

Applications consuming Eventflow output should be idempotent.

See [Delivery Semantics](docs/delivery-semantics.md) for the complete contract.

## Eventflow and Schema Registry

Eventflow does not replace Schema Registry.

Use Schema Registry for:

* centralized schema storage;
* schema versioning;
* compatibility policies;
* serializer and deserializer integration;
* Avro, Protobuf, or JSON Schema governance at the broker boundary.

Use Eventflow when the requirement also includes:

* HTTP or OpenLineage producers;
* CloudEvents envelope policy;
* producer isolation from broker credentials;
* invalid-event quarantine;
* durable pre-delivery acknowledgement;
* per-destination failure tracking;
* operator-controlled replay.

The two systems can be used together:

```text
OpenLineage producer
        |
        v
Eventflow admission
        |
        v
Kafka / Redpanda + Schema Registry
        |
        v
Eventflow delivery
        |
        v
Marquez
```

## Eventflow and Dapr

Dapr is optional and complementary.

Dapr can provide:

* service discovery;
* service invocation;
* workload mTLS;
* Pub/Sub abstractions;
* application resiliency policies.

Eventflow provides:

* event-contract enforcement;
* durable admission;
* invalid-event quarantine;
* destination delivery state;
* controlled replay.

A Dapr-managed path may look like:

```text
Dagster
  -> Dapr service invocation
  -> Eventflow HTTP ingress
  -> Redpanda
  -> Eventflow worker
  -> Marquez
```

Dapr forwards the request to Eventflow's application endpoint. Eventflow still owns validation and event admission.

A direct Dagster-to-Eventflow deployment does not require Dapr.

See [Dapr Integration](docs/integrations/dapr.md).

## Kubernetes

Eventflow runs as an ordinary Kubernetes workload and does not require an operator.

Typical deployment modes are:

* ingress gateway `Deployment`;
* Kafka delivery-worker `Deployment`;
* replay `Job`;
* configuration-validation `Job`.

Configuration can be mounted through ConfigMaps, while credentials should be supplied through Secrets or trusted workload identity.

SQLite-backed deployments require persistent storage and should run as a single active writer.

See [Kubernetes](docs/kubernetes.md).

## Scope

Eventflow intentionally does not provide:

* workflow orchestration;
* joins, windows, or aggregation;
* arbitrary payload-transformation languages;
* infrastructure provisioning;
* Kafka topic creation;
* broker administration;
* schema-registry administration;
* service discovery;
* secrets management;
* data cataloging;
* lineage visualization;
* lineage-semantic reconstruction;
* a Kubernetes operator;
* a distributed control plane.

Use specialized systems for those responsibilities.

## Go SDK

Eventflow can be run as a standalone gateway or embedded in a Go application.

The SDK is intentionally small and based on explicit dependency injection. It does not use hidden global adapter registration or `init()` side effects.

Module path:

```text
github.com/rezarajan/eventflow
```

Applications explicitly register the resource kinds and adapters they support, load resources, and compile the runtime graph.

The standalone CLI is the recommended starting point. Embed the SDK when Eventflow must participate directly in an existing Go process or when a custom adapter is required.

## Project Status

Eventflow is pre-1.0.

The current focus is:

* CloudEvents and OpenLineage validation;
* durable single-instance admission;
* Kafka and Redpanda integration;
* Marquez delivery;
* quarantine;
* retries;
* replay;
* operationally explicit delivery semantics.

Interfaces and resource schemas may change before the first stable release.

## Documentation

* [Architecture](docs/architecture.md)
* [Delivery semantics](docs/delivery-semantics.md)
* [Contracts](docs/contracts.md)
* [OpenLineage](docs/openlineage.md)
* [Operations](docs/operations.md)
* [Replay](docs/replay.md)
* [Security](docs/security.md)
* [Adapters](docs/adapters.md)
* [Kubernetes](docs/kubernetes.md)
* [Dapr integration](docs/integrations/dapr.md)
* [Migration](docs/migration.md)

## Development

Run the complete test suite:

```bash
go test ./...
go test -race ./...
go vet ./...
```

Validate that module dependencies are clean:

```bash
go mod tidy
git diff --exit-code -- go.mod go.sum
```

Start the local infrastructure:

```bash
just up-all
```

Stop it when finished:

```bash
just down
```

## Contributing

Changes should preserve Eventflow's narrow product boundary.

Before proposing a new adapter or resource kind, establish that it materially improves one of Eventflow's core scenarios:

* governed event admission;
* OpenLineage transport;
* quarantine;
* recoverable delivery;
* replay.

General-purpose transformation, workflow, provisioning, and integration-platform features are out of scope.

See [CONTRIBUTING.md](CONTRIBUTING.md) for development and submission requirements.
