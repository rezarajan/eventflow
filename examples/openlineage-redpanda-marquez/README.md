# OpenLineage Redpanda Marquez Example

This example demonstrates:

1. valid OpenLineage HTTP input reaching Redpanda and Marquez;
2. invalid input being rejected or quarantined;
3. Marquez outage causing retained journal state;
4. automatic retry or operator replay after Marquez returns.

```bash
just up-all
just topic openlineage.events.v1
go run ./cmd/eventflow run --config examples/openlineage-redpanda-marquez/ingress.yaml
go run ./cmd/eventflow run --config examples/openlineage-redpanda-marquez/worker.yaml
```

Post a valid event:

```bash
curl -fsS -X POST http://localhost:8080/events \
  -H 'content-type: application/json' \
  --data @examples/openlineage-redpanda-marquez/valid-run-event.json
```

Post an invalid event:

```bash
curl -i -X POST http://localhost:8080/events \
  -H 'content-type: application/json' \
  --data @examples/openlineage-redpanda-marquez/invalid-run-event.json
```

Replay failed Marquez deliveries:

```bash
go run ./cmd/eventflow replay \
  --config examples/openlineage-redpanda-marquez/worker.yaml \
  --destination HTTPEmitter/marquez \
  --state FAILED
```
