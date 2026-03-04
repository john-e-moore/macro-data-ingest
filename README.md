# Macro Data Ingest (BEA PCE by State)

This repository contains a lightweight data engineering pipeline for **BEA Personal Consumption Expenditures (PCE) by State**.

The target architecture is:
- **S3 Bronze** for immutable raw API payloads
- **S3 Silver** for cleaned, typed, normalized tables
- **S3 Gold** for analytics-ready modeled outputs
- **Postgres (RDS)** for curated serving tables and views

The pipeline is designed to be idempotent, observable, and reproducible, with daily runs orchestrated by GitHub Actions.

## Current Status

Implemented and validated vertical slices:
1. Provisioning and environment bootstrap (staging resources created)
2. Ingestion (BEA -> Bronze + manifests + hash-based change detection)
3. Silver transforms + baseline quality checks
4. Gold conformed model (dims/fact) + Postgres load + OBT/YoY serving views

Remaining: final CI/scheduler hardening and production rollout checks.

## Quickstart (Local)

1. Create Python environment:
   - `python -m venv .venv`
   - `source .venv/bin/activate`
2. Install project in editable mode:
   - `pip install -e .[dev]`
3. Copy and populate environment variables:
   - `cp .env.template .env`
4. Inspect available commands:
   - `mdi --help`
5. Run local checks:
   - `make test`
   - `make lint`

## Provision AWS Resources

After filling `.env`, run:
- `python scripts/provision_aws.py --env staging` (plan mode, no changes)
- `python scripts/provision_aws.py --env staging --apply`
- `python scripts/provision_aws.py --env prod --apply`

The script prints a summary of resource names/identifiers that should be copied into `.env` and GitHub secrets.

Provisioning behavior details and rollback guidance: `docs/setup.md`.

## Pipeline Commands

The CLI exposes pipeline commands:
- `mdi ingest --env staging --run-id <run_id>`
- `mdi transform --env staging --run-id <run_id>`
- `mdi load --env staging --run-id <run_id>`
- `mdi run-all --env staging --run-id <run_id>`
- `mdi run-all --env staging --run-id <run_id> --dataset-id pce_state_sapce1`
- `mdi run-all --env staging --run-id <run_id> --dataset-id pce_state_sapce4`
- `mdi run-all --env staging --run-id <run_id> --dataset-id pce_state_sapce4_monthly`

By default, ingest requests BEA annual years from `BEA_START_YEAR` through current year
(set in `.env`, default `2000`). `run-all` skips transform/load automatically when the
ingest payload hash is unchanged.

`run-all` reads `config/datasets.yaml` and processes each enabled dataset. The default config ingests:
- `pce_state_sapce1` (annual SAPCE1, `bea_frequency: A`)
- `pce_state_sapce4` (annual SAPCE4, `bea_frequency: A`)

The repo also includes a staged monthly config entry:
- `pce_state_sapce4_monthly` (monthly SAPCE4, `bea_frequency: M`, currently disabled by default)

Both annual datasets use `line_code: ALL`, so all function categories for each configured BEA table
are ingested for the enabled grain.
The monthly entry is disabled until BEA returns `TimePeriod` values at monthly grain for the requested table.

These commands are implemented end-to-end for staging and can be used in GitHub Actions
or local runs.

## CI and Scheduling

- CI workflow: `.github/workflows/ci.yml`
  - Runs linting and tests on PRs/pushes.
- Daily ingest workflow: `.github/workflows/ingest.yml`
  - Supports both scheduled and manual runs.
  - Uses repository secrets for AWS, BEA, and Postgres connectivity.

## Documentation

- Setup guide: `docs/setup.md`
- Architecture: `docs/architecture.md`
- Functional/non-functional spec: `docs/spec.md`
- Roadmap: `docs/roadmap.md`
- Operability runbook: `docs/operability.md`
- Backfill SOP: `docs/backfills.md`

## Data Modeling Strategy

Postgres uses a hybrid model:

- `gold` stores conformed dimensions and `gold.fact_macro_observation` as the durable core.
- `serving` exposes denormalized OBT-style views for common analyst/API access patterns.
- Legacy `gold.pce_state_annual` and `serving.v_pce_state_yoy` remain for compatibility while
  consumers migrate to generalized serving contracts.

## Security Notes

- Never commit secrets.
- Keep `.env` local only.
- Use least-privilege IAM roles and restricted security groups.