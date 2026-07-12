# Security

## Trust Model

Eventflow treats producers as outside the governed event boundary. Producers must authenticate through deployment-provided middleware, ingress, service mesh or sidecar policy.

## Authentication

Eventflow exposes authentication as an integration boundary. Do not embed long-lived identity policy in manifests. Use reverse proxies, Kubernetes ingress, service mesh, Dapr service invocation or application middleware.

## TLS And mTLS

Run HTTP behind an ingress/service mesh for TLS or mTLS when possible. Kafka/Redpanda TLS and SASL should use the selected client configuration and secrets mounted through files or environment variables.

## Secrets

Use Kubernetes Secrets, environment variables or mounted files. Eventflow is not a secrets manager.

## Payload Logging

Do not log full event payloads by default. Log stable identifiers and error classes.

## Denial Of Service Controls

Configure request-size limits, read/write/idle timeouts, bounded request concurrency, Kafka batch limits and dispatcher worker limits.

## Supply Chain

Keep Go modules pinned through `go.sum`, scan container images and avoid runtime schema downloads.
