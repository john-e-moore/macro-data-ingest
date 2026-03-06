# Functional and Non-Functional Spec

## Functional Requirements

1. Ingest BEA PCE by State via API with configurable categories and parameters.
2. Persist immutable raw payloads and request metadata to Bronze S3 paths.
3. Transform Bronze data into typed and normalized Silver tables.
4. Model Silver data into analytics-ready Gold outputs (Parquet preferred).
5. Load curated Gold data into Postgres conformed dimensions and fact tables with idempotent upsert behavior.
6. Create/refresh denormalized serving views (OBT-style) for common derivatives (YoY, MoM, rolling windows where applicable).
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
- **Model Stability**
  - Conformed dimensional contracts in `gold`; consumer-oriented denormalized contracts in `serving`.
- **Security**
  - No secrets in repo; encrypted storage and least-privilege IAM.
- **Cost Control**
  - Lifecycle and retention policies; avoid full reload when unnecessary.

## Dataset Scope

Initial scope:
- BEA state-level macro tables in the `Regional` dataset, including:
  - PCE by state (`SAPCE1`, `SAPCE4`)
  - Regional price parities by state (`SARPP`)
  - Real personal income and real PCE by state (`SARPI`)
  - State annual GDP table group (`SAGDP1`-`SAGDP9`, `SAGDP11`, `SASUMMARY`)
  - Personal transfer receipts to individuals from governments (`SAINC35`, line `2000`)
- Census state-level annual datasets, including:
  - State resident population (`acs/acs1`, variable `B01003_001E`, with intercensal backfill for pre-2005)
  - State government finance federal intergovernmental revenue (`timeseries/govs`, predicates `SVY_COMP=02`, `GOVTYPE=002`, `AGG_DESC=SF0004`)

Config-driven dimensions:
- frequency (annual and monthly supported)
- dataset/table identifiers
- geography selection (all states by default)
- optional category allowlist
- start year (`*_start_year`), with `2000` as the default baseline for new series

Canonical dataset definitions are maintained in `config/datasets.yaml` and summarized in
`docs/datasets.md`.

Future sources (BLS, IRS) are tracked separately and not part of MVP.
