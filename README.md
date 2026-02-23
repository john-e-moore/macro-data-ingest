# Macro Data Ingest (BEA PCE by State)

This repository contains a lightweight data engineering pipeline for **BEA Personal Consumption Expenditures (PCE) by State**.

The target architecture is:
- **S3 Bronze** for immutable raw API payloads
- **S3 Silver** for cleaned, typed, normalized tables
- **S3 Gold** for analytics-ready modeled outputs
- **Postgres (RDS)** for curated serving tables and views

The pipeline is designed to be idempotent, observable, and reproducible, with daily runs orchestrated by GitHub Actions.

## Current Status

Scaffolding and documentation are in place. Implementation will proceed in commit-ready vertical slices:
1. Provisioning and environment bootstrap
2. Ingestion (BEA -> Bronze + manifests + change detection)
3. Silver transforms + quality checks
4. Gold modeling + Postgres load + views
5. CI hardening and smoke paths

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
- `python scripts/provision_aws.py --env staging`
- `python scripts/provision_aws.py --env prod`

The script prints a summary of resource names/identifiers that should be copied into `.env` and GitHub secrets.

Provisioning behavior details and rollback guidance: `docs/setup.md`.

## Pipeline Commands (Scaffolded)

The CLI exposes staged pipeline commands:
- `mdi ingest --env staging --run-id <run_id>`
- `mdi transform --env staging --run-id <run_id>`
- `mdi load --env staging --run-id <run_id>`
- `mdi run-all --env staging --run-id <run_id>`

At scaffold stage, these commands are structured and wired but not fully implemented.

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

## Security Notes

- Never commit secrets.
- Keep `.env` local only.
- Use least-privilege IAM roles and restricted security groups.