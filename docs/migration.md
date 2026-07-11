# Migration Notes

Eventflow now uses `eventflow.dev/v1alpha1` resources as the only runtime
configuration model. The old registry-driven commands and generator runtime
paths have been removed.

Use these replacements:

| Previous idea | Current model |
| --- | --- |
| Registry event entry | `EventContract` resource. |
| Registry channel/adapter config | Explicit adapter resources such as `RedpandaEmitter` or `FilesystemReceiver`. |
| Registry-driven flow wiring | `EventFlow` for CloudEvents sources. |
| Platform notification ingestion | `ObservationFlow` with an `Observer` and `ObservationMapper`. |
| Generator command/runtime | Example or test tooling outside the SDK runtime path. |

Resource validation is explicit and startup-bound. Backing infrastructure such
as topics, buckets, queues, databases, and credentials is still managed outside
Eventflow.
