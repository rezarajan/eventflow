# Migration

| Current component | New abstraction | Compatibility action | Breaking change | Rationale |
| --- | --- | --- | --- | --- |
| HTTP ingress | `httpflow.Receiver` plus runtime handler | Existing command remains | Public SDK uses constructor config | Separate transport from validation and dispatch. |
| Fanout | `eventflow.Emitter` / `BatchEmitter` | `eventflow-fanout` remains | `Publisher` terminology is deprecated | Use transport-neutral names. |
| Redpanda output | `redpanda.Emitter` | Existing adapter wrapped | Public package path changes | Make adapter importable. |
| Redpanda source | `redpanda.Receiver` | Existing adapter wrapped | Manual commit semantics remain constrained | Public receiver interface. |
| log/stdout/discard outputs | filesystem/stdout command paths | Existing commands remain | Public SDK focuses on emitters | Keep data streaming simple. |
| JSONL/object handlers | `filesystem.Emitter` / `Receiver` | Behavior preserved in public package | Names move away from handler for outputs | Handler means application callback. |
| DuckDB handler | `duckdb.Emitter` | Existing projector wrapped | Receiver is constrained to Eventflow-owned tables | Avoid claiming general CDC. |
| lineage file | `lineage` helpers plus filesystem storage | Existing lineage file remains | Public lineage package path changes | Make OpenLineage importable. |
| Marquez | native OpenLineage HTTP emission | Existing replay command remains | Raw OpenLineage limited to native endpoints | Keep Eventflow transport as CloudEvents. |
| registry validate/AsyncAPI | `contract` registry v2 | v1 accepted as migration input | v2 channel is structured | Stronger registry validation. |

