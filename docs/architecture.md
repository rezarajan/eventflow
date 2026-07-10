# Architecture

Eventflow is now organized as a public Go SDK plus thin runtime commands.
The SDK owns ports, validation, codecs, lineage helpers, and importable
adapters. Commands compose those pieces and keep runtime policy in flags and
`EVENTFLOW_*` environment variables.

```mermaid
flowchart LR
  App[Application] --> SDK[eventflow SDK]
  SDK --> Ports[Emitter / Receiver / Observer / Validator / Codec]
  Ports --> FS[filesystem]
  Ports --> HTTP[httpflow]
  Ports --> RP[redpanda]
  Ports --> S3[s3]
  Ports --> Duck[duckdb]
  SDK --> Contract[contract registry v2]
  SDK --> CE[cloudevent]
  SDK --> OL[lineage]
```

```mermaid
flowchart TD
  A[Decode transport representation] --> B[Validate CloudEvents syntax]
  B --> C[Resolve registry entry]
  C --> D[Validate envelope attributes and extensions]
  D --> E[Validate payload JSON Schema]
  E --> F[Validate OpenLineage semantics when applicable]
  F --> G[Custom validators]
  G --> H[Dispatch or emit]
```

The SDK does not provision infrastructure, own credentials, or implement a
Datascape control plane. Redpanda topics, S3 buckets, DuckDB files, and Marquez
instances are attached resources.

