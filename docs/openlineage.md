# OpenLineage

OpenLineage is Eventflow's primary built-in production profile.

## Supported Input

Eventflow accepts OpenLineage run events as JSON payloads and can wrap them as CloudEvents with:

- type: `io.openlineage.run-event.v1`
- source: configured HTTP receiver source convention or `urn:eventflow:http`
- data: original OpenLineage run event

Structured CloudEvents using the same type are also supported.

## Producer Responsibility

Dagster, Spark, dbt and custom producers remain responsible for accurate OpenLineage semantics. Eventflow does not synthesize missing jobs, datasets, parent-child relationships, runtime facets or dataset discovery.

## Redpanda Transport

The ingress flow journals valid events before publishing to Redpanda. The worker flow consumes CloudEvents from Redpanda, journals before offset commit and delivers to Marquez.

## Marquez Delivery

Use `HTTPEmitter` with `mode: native-openlineage` to post the original OpenLineage payload to a Marquez-compatible endpoint.

## Outage And Replay

If Marquez is unavailable, delivery rows remain pending or failed in SQLite. The dispatcher retries automatically until terminal failure. Operators can replay failed records with `eventflow replay`.

## Non-Goal

Eventflow does not reconstruct runtime lineage semantics. It transports, validates, journals, quarantines and replays the semantics supplied by producers.
