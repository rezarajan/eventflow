# ADR 0003: Durable Journal

## Status

Accepted.

## Context

Accepted events must survive destination outages and process restarts.

## Decision

Use SQLite as the initial single-node durable journal with immutable event records and per-destination delivery state.

## Consequences

SQLite is simple and inspectable but single-writer/single-instance. Distributed journals are future work.

## Rejected Alternatives

- DuckDB as delivery coordination.
- Custom database.
- Distributed consensus in this refactor.
