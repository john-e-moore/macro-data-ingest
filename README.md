# Macro Data Ingest (BEA PCE by State)

This repository contains a lightweight data engineering pipeline for **state-level macro data** with current sources:
- **BEA Personal Consumption Expenditures (PCE) by State**
- **US Census annual state population (ACS 1-year with pre-2005 intercensal backfill)**
- **BEA SAINC35 annual transfer receipts to individuals from governments (line 2000)**
- **US Census Annual Survey of State Government Finance federal intergovernmental revenue (SVY_COMP=02, GOVTYPE=002, AGG_DESC=SF0004)**

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
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_regional_price_parities_sarpp`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_real_income_and_pce_sarpi`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp1`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp2`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp3`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp4`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp5`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp6`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp7`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp8`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp9`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sagdp11`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_gdp_sasummary`
- `mdi run-all --env staging --run-id <run_id> --dataset-id pce_state_sapce4_monthly`
- `mdi run-all --env staging --run-id <run_id> --dataset-id census_state_population`
- `mdi run-all --env staging --run-id <run_id> --dataset-id state_personal_transfer_receipts_sainc35`
- `mdi run-all --env staging --run-id <run_id> --dataset-id census_state_gov_finance_federal_intergovernmental_revenue`

By default, ingest requests BEA annual years from `BEA_START_YEAR` through current year
(set in `.env`, default `2000`). `run-all` skips transform/load automatically when the
ingest payload hash is unchanged.

`run-all` reads `config/datasets.yaml` and processes each enabled dataset. The default config ingests:
- `pce_state_sapce1` (annual SAPCE1, `bea_frequency: A`)
- `pce_state_sapce4` (annual SAPCE4, `bea_frequency: A`)
- `state_regional_price_parities_sarpp` (annual SARPP, `bea_frequency: A`)
- `state_real_income_and_pce_sarpi` (annual SARPI, `bea_frequency: A`, includes real PI, real PI per capita, real PCE, and real PCE per capita)
- `state_gdp_sagdp1` through `state_gdp_sagdp9` (annual SAGDP table group, `bea_frequency: A`)
- `state_gdp_sagdp11` (annual SAGDP table group, `bea_frequency: A`)
- `state_gdp_sasummary` (annual state GDP summary table, `bea_frequency: A`)
- `state_personal_transfer_receipts_sainc35` (annual SAINC35 line 2000 transfer receipts to individuals from governments)
- `census_state_gov_finance_federal_intergovernmental_revenue` (annual Census state government intergovernmental revenue, AGG_DESC `SF0004`)

The repo also includes a staged monthly config entry:
- `pce_state_sapce4_monthly` (monthly SAPCE4, `bea_frequency: M`, currently disabled by default)
- `census_state_population` (annual Census population, `census_frequency: A`, `census_start_year: 2000`)

Most annual BEA datasets use `line_code: ALL`, so all function categories for each configured BEA table
are ingested for the enabled grain; `state_personal_transfer_receipts_sainc35` is intentionally scoped to
`line_code: 2000`.
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
- Fiscal-intensity views are available in:
  - `serving.v_state_federal_to_stategov_gdp_annual`
  - `serving.v_state_federal_to_persons_gdp_annual`

Interpretation note:
- `v_state_federal_to_persons_gdp_annual` uses BEA `SAINC35` line `2000` (transfer receipts of individuals from governments), which can include non-federal government components.

## Security Notes

- Never commit secrets.
- Keep `.env` local only.
- Use least-privilege IAM roles and restricted security groups.