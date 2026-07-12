# Migration

## Product Boundary

Eventflow is now a schema-enforcing CloudEvents gateway. Documentation and examples no longer describe it as a general declarative runtime.

## Resource Changes

- Add `journalRef` to production `EventFlow` resources.
- Prefer `invalidEmitterRef`; `invalidEventRef` is a deprecated compatibility alias.
- Prefer `dataSchema`; `payloadSchema` is a deprecated compatibility alias.
- Add `SQLiteJournal` for durable accepted-event and delivery state.
- `DuckDBReceiver` is no longer registered. `DuckDBEmitter` remains optional analytical/raw export.

## Command Changes

Use the primary command:

```text
eventflow validate
eventflow inspect
eventflow run
eventflow replay
```

Standalone `eventflow-emit`, `eventflow-receive`, `eventflow-relay` and `eventflow-lineage-replay` are deprecated compatibility utilities.

## Delivery Semantics

Acknowledgement now happens after validation and durable journal append for journaled flows. Kafka offsets are committed by the runtime only after handler success.
