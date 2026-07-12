# Adapters

Supported core adapters:

| Resource | Purpose |
| --- | --- |
| `HTTPReceiver` | HTTP CloudEvents/OpenLineage ingress |
| `HTTPEmitter` | HTTP CloudEvents or native OpenLineage delivery |
| `RedpandaReceiver` | Kafka-compatible CloudEvents source |
| `RedpandaEmitter` | Kafka-compatible CloudEvents destination |
| `FilesystemReceiver` | Development, fixtures and incident recovery |
| `FilesystemEmitter` | Development output and quarantine |
| `SQLiteJournal` | Single-node durable gateway journal |

Optional narrow adapters:

| Resource | Scope |
| --- | --- |
| `S3NotificationObserver` and mapper | One observation-to-event example for object-created notifications |
| `DuckDBEmitter` | Analytical/raw export only, not transactional delivery coordination |

Eventflow connects to existing Kafka/Redpanda topics. It does not create topics, provision buckets or administer brokers.
