# Architecture

Eventflow is organized as a public Go SDK plus a declarative resource runtime.
The SDK owns ports, validation, codecs, lineage helpers, importable adapters,
and resource compilation. Commands load resource YAML, compile it, and run the
resulting flow.

```mermaid
flowchart LR
  App[Application] --> SDK[eventflow SDK]
  SDK --> Ports[Emitter / Receiver / Observer / Mapper / Validator / Codec]
  Ports --> FS[filesystem]
  Ports --> HTTP[httpflow]
  Ports --> RP[redpanda]
  Ports --> S3[s3]
  Ports --> Duck[duckdb]
  SDK --> Resource[eventflow.dev/v1alpha1 resources]
  SDK --> CE[cloudevent]
  SDK --> OL[lineage]
```

```mermaid
flowchart TD
  A[Load YAML resources] --> B[Validate resource envelopes and specs]
  B --> C[Build dependency graph]
  C --> D[Check references and capabilities]
  D --> E[Compile components and flows]
  E --> F[Run EventFlow or ObservationFlow]
  F --> G[Validate CloudEvents contracts]
  G --> H[Emit accepted events]
```

The SDK does not provision infrastructure, own credentials, or implement a
Datascape control plane. Redpanda topics, S3 buckets, DuckDB files, and Marquez
instances are attached resources.
