# Quarantine

Rejected events are retained in `QuarantineStore` with immutable IDs, original bytes, SHA-256 digest, CloudEvents ID when available, OpenLineage run ID when available, principal, policy and contract versions, decision, reason code, field path, receive time, target, replay history, and resolution status.

Commands:

```bash
eventflow quarantine list --config resources.yaml
eventflow quarantine show --config resources.yaml <id>
eventflow quarantine validate --config resources.yaml <id>
eventflow quarantine replay --config resources.yaml --event corrected.json <id>
eventflow quarantine dismiss --config resources.yaml <id>
```

Replay preserves the original CloudEvents identity by default. Use `--replace-identity` only when the operator intentionally wants a new CloudEvents ID. Replay writes an audit row and marks the record replayed.

Normal logs must not include full sensitive payloads. Use `show --payload` only for controlled operator inspection.
