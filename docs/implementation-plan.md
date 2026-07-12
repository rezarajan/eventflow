# Implementation Plan

1. Baseline and design: inventory packages, commands, public APIs, tests and unsupported placeholders.
2. Product cleanup: rewrite README/docs, consolidate command behavior and remove placeholder registrations.
3. Journal and delivery: add SQLite journal, per-destination delivery state, dispatcher, retry and replay.
4. HTTP and Kafka hardening: enforce HTTP acknowledgement and Kafka offset commit after journal append.
5. OpenLineage profile: provide runnable HTTP -> SQLite -> Redpanda -> SQLite -> Marquez example.
6. Operations and Kubernetes: expose metrics, health/readiness and ordinary Kubernetes manifests.
7. Verification: run tests, race tests, vet and the local OpenLineage demo.
