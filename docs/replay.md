# Replay

Replay resends selected journaled events to one configured destination.

```bash
eventflow replay \
  --config examples/openlineage-redpanda-marquez/worker.yaml \
  --destination HTTPEmitter/marquez \
  --state FAILED \
  --limit 100
```

## Filters

- `--flow`: select one flow when multiple flows are compiled.
- `--destination`: required destination resource key, for example `HTTPEmitter/marquez`.
- `--state`: `FAILED`, `PENDING`, `DELIVERED` or `all`.
- `--event-id`: CloudEvents ID.
- `--limit`: maximum records.
- `--dry-run`: print selected records without emitting.

## Identity

Replay preserves the original CloudEvents ID, source, type and timestamp.

## Duplicates

Replay can duplicate downstream delivery. Sinks must be idempotent.

## Audit Output

Dry-run output includes journal record ID, CloudEvents ID, type and source.
