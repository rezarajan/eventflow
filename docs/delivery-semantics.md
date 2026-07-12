# Delivery Semantics

Eventflow's supported guarantee is at-least-once delivery after an event has been durably journaled.

## HTTP Acknowledgement Boundary

HTTP ingress returns success only after:

1. the request is decoded;
2. a contract is resolved;
3. CloudEvents and payload validation succeed;
4. the accepted event is appended to the durable journal.

Downstream delivery may happen asynchronously after acknowledgement. Malformed or invalid input receives a non-2xx response unless an explicit quarantine policy accepts it.

## Kafka Commit Boundary

Kafka/Redpanda source offsets are committed only after the runtime handler succeeds. For journaled flows, that means after durable journal append. A crash before commit can redeliver the same message.

## Destination Failure

Delivery state is tracked per destination. If one destination succeeds and another fails, the successful destination remains `DELIVERED` while the failed destination retries or becomes `FAILED`.

## Retry And Terminal Failure

The dispatcher applies bounded exponential backoff with jitter. `maxAttempts` controls terminal failure. Terminal failures remain replayable through `eventflow replay`.

## Ordering

Eventflow does not claim global ordering. Ordering is limited by receiver behavior, partitioning and dispatcher concurrency.

## Duplicates And Idempotency

Duplicate delivery is possible after crashes, retries and replay. Sinks should be idempotent by CloudEvents `id`, an idempotency header or destination-specific key.

## Shutdown And Recovery

Graceful shutdown stops receivers, drains in-flight dispatcher work up to the configured timeout and leaves uncompleted delivery rows pending. On restart, pending rows are eligible for dispatch.

## Replay

Replay selects journaled records by flow, destination, state, event ID and limit. Replay preserves the original CloudEvents identity and timestamp.
