# ADR: Use OpenLineage for Lineage

## Status

Accepted for standards-only increment.

## Context

Lineage is the primary proof point for this PoC. A standard lineage contract avoids coupling pipelines to Marquez or any other backend.

## Decision

Use OpenLineage run events, datasets, jobs, inputs, outputs, and facets as the lineage contract.

## What We Will Not Customize

Do not create a custom Lineage Service API.

## Swap-Out Implication

Marquez can be replaced by any backend or transport path that accepts OpenLineage-compatible events.

## Accepted Limitations for PoC

Custom OpenLineage facets may be added only when standard facets are insufficient and must be versioned.
