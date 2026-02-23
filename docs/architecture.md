# Architecture

## Overview

The pipeline follows a lightweight lakehouse pattern:
- **Bronze**: immutable raw BEA API payloads
- **Silver**: cleaned, typed, normalized records
- **Gold**: analytics-ready modeled outputs
- **Postgres**: curated serving tables and derived views

Orchestration is handled by GitHub Actions on a daily schedule plus manual dispatch.

## Bronze / Silver / Gold Definitions

- **Bronze**
  - Raw JSON payloads and request metadata
  - Immutable write-once lineage records per run
- **Silver**
  - Stable schemas and typed columns
  - Normalized keys for geography/time/category
- **Gold**
  - Consumer-friendly facts/aggregates suitable for BI and SQL analysis

## S3 Partitioning Conventions

Root pattern:

`s3://<bucket>/<env>/<layer>/<source>/<dataset>/<extract_date>/...`

Examples:
- `s3://tlg-macro-data/staging/bronze/bea/pce_state/2026-02-23/payload.json`
- `s3://tlg-macro-data/staging/silver/bea/pce_state/year=2025/state=CA/part-000.parquet`
- `s3://tlg-macro-data/staging/gold/bea/pce_state_metrics/year=2025/part-000.parquet`

Design notes:
- Deterministic keys for reproducibility
- Stable partitioning to support idempotent overwrite behavior
- Run-level manifests stored alongside outputs for lineage

## Serving Model (Postgres)

Recommended schemas:
- `meta`: ingestion runs, manifests, checkpoint metadata
- `gold`: curated fact tables and dimensions
- `serving` (optional): analyst-facing views

Typical objects:
- `meta.ingest_runs`
- `gold.pce_state_annual`
- `serving.v_pce_state_yoy`
- `serving.v_pce_state_mom` (when monthly grain is enabled)

## Environment Strategy

Two isolated environments:
- `staging`: validation and smoke workloads
- `prod`: scheduled production workloads

Isolation strategy:
- Environment prefixes in S3
- Environment-specific IAM roles
- Separate RDS instances or strict schema isolation
- Distinct GitHub secret values per environment

## Lightweight Lineage

Each run persists:
- source endpoint + query params
- extraction timestamp
- content hash/checksum
- row counts and quality check outcomes
- output partitions generated

Lineage is written to both S3 manifests and Postgres metadata tables.
