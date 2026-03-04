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
  - Current slice keeps state + DC rows with keys: `bea_table_name`, `state_fips`, `state_abbrev`, `year`, `line_code`
- **Gold**
  - Conformed dimensions + fact table as the durable semantic core
  - Legacy wide table (`gold.pce_state_annual`) retained for compatibility
  - Serving-facing denormalized OBT views in `serving`

## S3 Partitioning Conventions

Root pattern:

`s3://<bucket>/<env>/<layer>/<source>/<dataset>/<extract_date>/...`

Examples:
- `s3://tlg-macro-data/staging/bronze/bea/pce_state/extract_date=2026-03-03/run_id=<run_id>/payload.json`
- `s3://tlg-macro-data/staging/silver/bea/pce_state/extract_date=2026-03-03/run_id=<run_id>/part-000.parquet`
- `s3://tlg-macro-data/staging/gold/bea/pce_state_metrics/year=2025/part-000.parquet`

Design notes:
- Deterministic keys for reproducibility
- Stable partitioning to support idempotent overwrite behavior
- Run-level manifests stored alongside outputs for lineage

## Serving Model (Postgres)

Recommended schemas and contracts:
- `meta`: ingestion runs, manifests, checkpoint metadata
- `gold`: conformed dimensions and fact tables (system of record)
- `serving`: analyst/API-facing denormalized views

Typical objects:
- `meta.ingest_runs`
- `gold.dim_source`
- `gold.dim_geo`
- `gold.dim_period`
- `gold.dim_series`
- `gold.dim_series_node`
- `gold.bridge_series_node`
- `gold.dim_vintage`
- `gold.fact_macro_observation`
- `gold.pce_state_annual` (compatibility table during migration)
- `serving.obt_state_macro_annual_latest`
- `serving.v_macro_yoy`
- `serving.v_pce_state_yoy` (compatibility projection)

### Conformed Core Model

`gold.fact_macro_observation` stores one measure per row at observation grain:

- Keys: `source_id`, `series_id`, `geo_id`, `period_id`, `vintage_id`
- Measures: `value_numeric`, `value_scaled`
- Metadata: `note_ref`, `run_id`, `loaded_at`

Dimension responsibilities:

- `dim_source`: source + dataset identity (e.g., `BEA` + `pce_state_sapce4`)
- `dim_geo`: geography keys (`state_fips`, `state_abbrev`, `geo_name`)
- `dim_period`: period semantics (`frequency`, `period_code`, `year`, optional `month`/`quarter`, bounds)
- `dim_series`: semantic measure identity (`series_code`, `line_code`, compatibility labels, units, raw label)
- `dim_series_node`: canonical hierarchical taxonomy nodes per BEA table
- `bridge_series_node`: bridge linking each series to its full hierarchy path
- `dim_vintage`: release/as-of tracking and `is_latest` selection flag

### Serving Model

Use denormalized views for consumer ergonomics and performance:

- `serving.obt_state_macro_annual_latest`: one wide annual row at latest vintage
- `serving.v_macro_yoy`: generalized YoY derivation from OBT
- `serving.v_pce_state_yoy`: BEA-compatible projection for existing consumers

Design principle:
- Keep reusable semantics in conformed `gold` objects.
- Publish user-friendly, query-optimized contracts in `serving`.

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

## Multi-table Daily Ingest

Dataset definitions are kept in `config/datasets.yaml`. Each enabled entry controls:
- BEA table (`bea_table_name`)
- query shape (`line_code`, `geo_fips`, `bea_frequency`, start year)
- storage identity (`dataset_id`)
- Postgres target table

Daily runs iterate all enabled datasets, so adding a BEA table or a new time grain is config-first.
Current enabled annual datasets are `pce_state_sapce1` and `pce_state_sapce4`; monthly SAPCE4
remains staged as a disabled entry.
