# OpenLineage

Eventflow transports OpenLineage through one CloudEvents type:
`io.openlineage.run-event.v1`. The lifecycle remains in
`data.eventType`: `START`, `COMPLETE`, `FAIL`, or `ABORT`.

```mermaid
flowchart LR
  CE[CloudEvent type io.openlineage.run-event.v1] --> Data[data]
  Data --> ET[eventType START/COMPLETE/FAIL/ABORT]
  Data --> Run[run]
  Data --> Job[job]
  Data --> Parent[parent facet]
```

Raw OpenLineage HTTP emission remains available for native OpenLineage
endpoints such as Marquez.

