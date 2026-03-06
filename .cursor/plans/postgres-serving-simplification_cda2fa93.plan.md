---
name: postgres-serving-simplification
overview: Simplify Postgres to conformed core + serving contracts by removing compatibility tables/views and dual-write logic, then validate end-to-end idempotent ingest/transform/load behavior.
todos:
  - id: agent-plan-artifacts
    content: Create feature brief and ExecPlan entries per .agent standards
    status: completed
  - id: remove-compat-ddl-and-writes
    content: Remove compatibility table/view DDL and dual-write load path
    status: completed
  - id: simplify-config-contract
    content: Remove or deprecate target_table config coupling and update related tests
    status: completed
  - id: docs-cleanup
    content: Update architecture/setup/operability/backfill docs to conformed-only model
    status: completed
  - id: db-drop-and-validation
    content: Drop legacy DB objects and run lint/tests + staging idempotency validations
    status: completed
isProject: false
---

# Postgres Simplification Plan

## Scope and Assumptions

- We will simplify the serving model now (pre-frontend), with no requirement to preserve backward compatibility objects.
- We will keep `gold` conformed model as system of record and keep `serving` consumer views that derive from conformed tables.
- We will follow `.agent` workflow requirements: feature brief + ExecPlan updates, test evidence, and docs updates.

## Phase 1: Planning and Repo Workflow Alignment

- Create feature brief at `.agent/features/<date>-remove-compatibility-surfaces/SPEC.md` documenting:
  - removed objects,
  - acceptance criteria (idempotent reruns still pass),
  - rollback path (rebuild via S3 + rerun pipeline).
- Add/update ExecPlan section in `.agent/PLANS.md` with required sections from `.agent/PLANS.md` standard and explicit validation commands.
- Add active plan entry in the optional index section while work is in progress.

## Phase 2: Remove Compatibility Writes and Schema Surfaces

- Update load orchestration in `src/macro_data_ingest/load/pipeline.py`:
  - remove compatibility write path (`upsert_gold_table(...)`),
  - keep conformed load (`upsert_conformed_observations(...)`) and serving refresh.
- Update loader DDL/view management in `src/macro_data_ingest/load/postgres_loader.py`:
  - remove compatibility table DDL blocks (`pce_state_annual`, `pce_state_monthly`, `population_state_annual`, `state_gov_finance_annual`),
  - remove migration `ALTER TABLE` blocks for those tables,
  - remove compatibility view creation (`serving.v_pce_state_yoy`) and any references to it.
- Keep and validate conformed/serving objects:
  - `meta.ingest_runs`,
  - `gold.dim_source`, `gold.dim_geo`, `gold.dim_period`, `gold.dim_series`, `gold.dim_series_node`, `gold.bridge_series_node`, `gold.dim_vintage`, `gold.fact_macro_observation`,
  - `serving.obt_state_macro_annual_latest`, `serving.v_macro_yoy`, `serving.v_pce_state_per_capita_annual`, `serving.v_state_federal_to_stategov_gdp_annual`, `serving.v_state_federal_to_persons_gdp_annual`.

## Phase 3: Remove Config/Test Coupling to Legacy Targets

- Update dataset config handling in `src/macro_data_ingest/datasets.py` and config files:
  - either remove `target_table` from required spec or keep as deprecated/no-op field with explicit documentation,
  - prefer removing it to reduce AI/user confusion, if test impact is manageable.
- Update config docs/samples:
  - `config/datasets.yaml`,
  - `config/datasets.example.yaml`.
- Update tests impacted by removal:
  - `tests/test_datasets.py`,
  - `tests/test_load_pipeline.py` (if assertions include target table fields/manifests),
  - any tests asserting compatibility objects or table names.

## Phase 4: Documentation and Runbook Cleanup

- Update user/operator docs to reflect the simplified contract:
  - `README.md`,
  - `docs/architecture.md`,
  - `docs/setup.md`,
  - `docs/operability.md`,
  - `docs/backfills.md` (remove/replace `gold.pce_state_annual` backfill references).
- Update statements that currently describe compatibility tables/views as active or recommended.

## Phase 5: Database Cleanup + Validation

- Execute DB cleanup (after code is ready):
  - `DROP VIEW IF EXISTS serving.v_pce_state_yoy;`
  - `DROP TABLE IF EXISTS gold.pce_state_monthly;`
  - `DROP TABLE IF EXISTS gold.pce_state_annual;`
  - `DROP TABLE IF EXISTS gold.population_state_annual;`
  - `DROP TABLE IF EXISTS gold.state_gov_finance_annual;`
- Run validation commands:
  - `make lint test PYTHON=.venv/bin/python`
  - `mdi run-all --env staging --run-id simplify-conformed-<date> --dataset-id pce_state_sapce4`
  - `mdi run-all --env staging --run-id simplify-conformed-<date> --dataset-id census_state_population`
  - immediate reruns for idempotency evidence.
- Validate SQL-level outcomes:
  - legacy objects absent,
  - conformed fact row counts stable on rerun,
  - serving views query successfully and return expected non-empty results where applicable.

## Acceptance Criteria

- Pipeline runs ingest/transform/load without writing any compatibility table.
- End-to-end reruns remain idempotent (no duplicate growth in `gold.fact_macro_observation`).
- Serving queries used by frontend/AI are unambiguous and sourced from conformed-backed views only.
- Docs and `.agent` plan artifacts reflect the new simplified architecture.

## Idempotence and Recovery

- Idempotence remains guaranteed by existing upsert keys in conformed dimensions/fact.
- If cleanup or deploy fails mid-way, recovery is to:
  - rerun `mdi load` (safe upserts),
  - recreate serving views via loader,
  - rebuild historical state from S3 Silver/Gold by rerunning pipeline per dataset.

## Primary Files Expected to Change

- `src/macro_data_ingest/load/pipeline.py`
- `src/macro_data_ingest/load/postgres_loader.py`
- `src/macro_data_ingest/datasets.py`
- `config/datasets.yaml`
- `config/datasets.example.yaml`
- `README.md`
- `docs/architecture.md`
- `docs/setup.md`
- `docs/operability.md`
- `docs/backfills.md`
- `tests/test_datasets.py`
- `tests/test_load_pipeline.py`
- `.agent/features/<date>-remove-compatibility-surfaces/SPEC.md`
- `.agent/PLANS.md`

