# Standards Register

This register is the controlling reference for the standards-only increment. The project must not create custom external APIs where an existing standard already owns the boundary.

| Boundary | Standard / Contract | First Local Implementation | Custom External API Allowed? | Notes |
|---|---|---|---:|---|
| Object storage | S3-compatible object API | MinIO later | No | Used for raw files, PDFs, Iceberg data files, and object metadata. |
| Lakehouse table format | Apache Iceberg table format | Iceberg tables later | No | Used for snapshots, schemas, table metadata, and table interoperability. |
| Catalog | Iceberg REST Catalog API | Local REST catalog later | No | Used for namespace and table metadata operations. |
| Data files | Apache Parquet | Parquet files later | No | Used for columnar lakehouse data files. |
| Lineage | OpenLineage event model and transport | Marquez later | No | Marquez is a backend, not the lineage contract. |
| Domain event envelope | CloudEvents | Redpanda/Kafka later | No custom envelope | Payloads may be project-specific but must sit inside the CloudEvents envelope. |
| Event-channel documentation | AsyncAPI | AsyncAPI document only in this increment | No | Describes future event bus channels and messages. |
| Observability | OpenTelemetry | OTel later | No | Used for logs, metrics, traces, and correlation IDs. |
| JSON payload validation | JSON Schema | Local validation later | Internal only | Used for generator config, event payloads, audit records, quality results, and conformance reports. |
| Synthetic operational source database | SQL DDL | PostgreSQL later | Internal only | Defines fake source-system tables for schools, students, attendance, grades, and documents. |
| Internal application boundaries | Go interfaces | Interfaces only | Internal only | Ports protect code from adapter details; they are not network APIs. |
| Platform HTTP API | OpenAPI | Deferred | Only if needed | Do not create a platform API until a use case exists that is not already covered by S3, Iceberg REST, OpenLineage, CloudEvents, AsyncAPI, or OpenTelemetry. |

## Non-negotiable design rule

Pipeline code may depend on internal ports and standards-compliant clients. It must not depend on implementation-specific APIs such as MinIO-specific extensions, Marquez-specific APIs, or future backend-specific APIs unless isolated behind an adapter.
