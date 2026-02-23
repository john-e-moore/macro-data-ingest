# Functional and Non-Functional Spec

## Functional Requirements

1. Ingest BEA PCE by State via API with configurable categories and parameters.
2. Persist immutable raw payloads and request metadata to Bronze S3 paths.
3. Transform Bronze data into typed and normalized Silver tables.
4. Model Silver data into analytics-ready Gold outputs (Parquet preferred).
5. Load curated Gold data into Postgres with idempotent upsert behavior.
6. Create/refresh serving views for common derivatives (YoY, MoM, rolling windows where applicable).
7. Execute daily via GitHub Actions with manual run support.
8. Reprocess only when source data changes (hash/vintage/checkpoint detection).

## Non-Functional Requirements

- **Idempotency**
  - Re-running with same input and run parameters must not duplicate records.
- **Reproducibility**
  - Deterministic partitioning and stable transforms.
- **Observability**
  - Structured logs with run identifiers and stage-level context.
- **Data Quality**
  - Row-count, non-null key, schema, and uniqueness checks.
- **Security**
  - No secrets in repo; encrypted storage and least-privilege IAM.
- **Cost Control**
  - Lifecycle and retention policies; avoid full reload when unnecessary.

## Dataset Scope

Initial scope:
- BEA PCE by State (configurable table/line categories and time ranges).

Config-driven dimensions:
- frequency (annual to start)
- dataset/table identifiers
- geography selection (all states by default)
- optional category allowlist

Future sources (BLS, Census, IRS) are tracked separately and not part of MVP.
