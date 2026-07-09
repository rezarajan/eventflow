# ADR: Use S3-Compatible Object Storage

## Status

Accepted for standards-only increment.

## Context

Object storage is required for raw extracts, generated PDFs, Iceberg data files, manifests, and auditable object metadata.

## Decision

Use the S3-compatible object API as the object storage boundary.

## What We Will Not Customize

Do not create a custom document storage API or object-storage API.

## Swap-Out Implication

MinIO can be replaced by AWS S3, Ceph, Cloudflare R2, or another S3-compatible implementation if the compatibility profile is satisfied.

## Accepted Limitations for PoC

The PoC profile uses only a subset of S3 operations.
