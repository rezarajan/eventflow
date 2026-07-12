# ADR 0005: No Kubernetes Operator

## Status

Accepted.

## Context

Eventflow needs Kubernetes deployment support but not a control plane.

## Decision

Provide ordinary manifests and jobs. Do not build an operator or reconciler.

## Consequences

Deployments remain transparent and easy to audit.

## Rejected Alternatives

- CustomResourceDefinitions.
- Controller-runtime operator.
