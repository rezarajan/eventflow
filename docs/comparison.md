# Comparison

Eventflow is an OpenLineage admission and quarantine gateway. It is intentionally narrower than mature event and integration platforms.

| Product | Where it is better | Where Eventflow fits |
| --- | --- | --- |
| Schema Registry | Schema storage, compatibility checks, producer and consumer schema negotiation | Identity-aware OpenLineage admission, namespace authorization, quarantine, and replay decisions |
| Strimzi Kafka Bridge | HTTP access to Kafka in Kubernetes | Governed OpenLineage admission before Kafka publication |
| Kafka Connect HTTP Sink | Moving Kafka records to HTTP sinks | Policy rejection, operator quarantine, and OpenLineage-specific admission |
| Redpanda Connect / Benthos | Arbitrary processing pipelines and connectors | A fixed OpenLineage gateway without a transformation DSL |
| Dapr | Service invocation, mTLS, discovery, Pub/Sub abstraction | Contract and policy enforcement at a lineage boundary |
| Knative Eventing | Kubernetes event routing and broker abstractions | Data-platform lineage admission and quarantine |
| OpenLineage native transports | Runtime event generation close to producers | Shared-infrastructure governance after producers emit events |

Eventflow does not replace Marquez. Marquez remains the lineage store and visualization system.
