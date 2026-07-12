# Eventflow

Eventflow is a schema-enforcing CloudEvents gateway for teams that operate shared event or lineage infrastructure.

Use it when events must be validated, normalized, durably recorded, quarantined and replayed before they enter Kafka-compatible brokers or downstream systems such as Marquez.

Eventflow is particularly useful when:

- multiple producers publish into shared event infrastructure;
- consumers require enforceable, versioned contracts;
- invalid events must be isolated before reaching downstream systems;
- producers must remain available while a destination is offline;
- OpenLineage events need reliable transport and replay.

Eventflow is not a workflow engine, stream processor, service mesh, Dapr replacement, infrastructure provisioner, schema registry, data catalog or runtime-semantics reconstruction system.

## Do I need Eventflow?

| Scenario                                                          | Use Eventflow? | Reason                                         |
| ----------------------------------------------------------------- | -------------: | ---------------------------------------------- |
| Dagster emits OpenLineage directly to a reliable Marquez endpoint |     Usually no | Direct integration is simpler                  |
| Marquez outages must not affect Dagster runs                      |            Yes | Journal, retry and replay isolate the producer |
| Several teams publish into a shared Redpanda topic                |            Yes | Central contract enforcement and quarantine    |
| One Go service publishes to one Kafka topic                       |     Usually no | Use the CloudEvents SDK and Kafka client directly |
| Complex joins, windows and aggregations                           |             No | Use a stream processor                         |
| Reliable multi-step business workflow                             |             No | Use Temporal or another workflow engine        |
| Existing Dapr application needs governed event admission          |       Possibly | Dapr handles connectivity; Eventflow handles governance and replay |

## Smallest Useful Deployment

The smallest production-shaped deployment is one `eventflow run` process with:

- an ingress receiver such as `HTTPReceiver` or `RedpandaReceiver`;
- one or more `EventContract` resources;
- a `SQLiteJournal`;
- one destination emitter;
- an invalid-event emitter for quarantine.

Accepted events are acknowledged only after they are decoded, matched to a contract, validated and durably journaled. Delivery to destinations happens after the journal boundary and is at least once.

## OpenLineage

OpenLineage is Eventflow's primary built-in profile. Producers such as Dagster, Spark, dbt and custom jobs remain responsible for producing accurate OpenLineage run events. Eventflow validates, journals, transports, quarantines, retries and replays those events without inventing jobs, datasets, facets or parent-child relationships.

## Dapr

Dapr is optional and complementary. Dapr can provide service invocation, mTLS, service discovery and Pub/Sub plumbing. Eventflow provides the governed boundary: contract validation, durable journal, quarantine, retry and replay. A direct Dagster-to-Eventflow deployment does not require Dapr.

## Quickstart: OpenLineage To Redpanda And Marquez

Start local infrastructure:

```bash
just up-all
just topic openlineage.events.v1
```

Run ingress in one terminal:

```bash
go run ./cmd/eventflow run --config examples/openlineage-redpanda-marquez/ingress.yaml
```

Post a valid OpenLineage run event:

```bash
curl -fsS -X POST http://localhost:8080/events \
  -H 'content-type: application/json' \
  --data @examples/openlineage-redpanda-marquez/valid-run-event.json
```

Run the delivery worker in another terminal:

```bash
go run ./cmd/eventflow run --config examples/openlineage-redpanda-marquez/worker.yaml
```

Replay failed records after a destination outage:

```bash
go run ./cmd/eventflow replay \
  --config examples/openlineage-redpanda-marquez/worker.yaml \
  --destination HTTPEmitter/marquez \
  --state FAILED
```

Inspect gateway resources without running them:

```bash
go run ./cmd/eventflow validate --config examples/openlineage-redpanda-marquez/ingress.yaml
go run ./cmd/eventflow inspect --config examples/openlineage-redpanda-marquez/ingress.yaml
```

## Resource Model

Eventflow resources use `eventflow.dev/v1alpha1` YAML. Core resources are `EventContract`, `EventFlow`, `SQLiteJournal`, transport receivers and transport emitters.

```yaml
apiVersion: eventflow.dev/v1alpha1
kind: EventContract
metadata:
  name: openlineage-run-event
spec:
  type: io.openlineage.run-event.v1
  dataSchema: ./schemas/openlineage-run-event.json
```

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

The compiler rejects unknown envelope fields, unknown spec fields, duplicate identities, missing references, dependency cycles, capability mismatches and invalid contract references.

## Commands

```text
eventflow validate --config resources.yaml
eventflow inspect --config resources.yaml
eventflow run --config resources.yaml
eventflow replay --config resources.yaml --destination HTTPEmitter/marquez --state FAILED
```

Standalone utility commands from earlier versions are deprecated. Use the primary `eventflow` command for validation, inspection, running and replay.

## Documentation

- [Architecture](docs/architecture.md)
- [Delivery semantics](docs/delivery-semantics.md)
- [Contracts](docs/contracts.md)
- [OpenLineage](docs/openlineage.md)
- [Operations](docs/operations.md)
- [Replay](docs/replay.md)
- [Security](docs/security.md)
- [Adapters](docs/adapters.md)
- [Kubernetes](docs/kubernetes.md)
- [Dapr integration](docs/integrations/dapr.md)
- [Migration](docs/migration.md)
