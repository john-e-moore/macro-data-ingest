# Feature Brief: Standardized Backfill Process and `function_name` Repair

## Context

`gold.pce_state_annual` includes rows (notably SAPCE4 and historical SAPCE3) where
`function_name` is blank because data was loaded before function-name propagation was implemented.
The repository has backfill principles but lacks a concrete SOP and execution utility.

## Scope

- Add a standardized backfill runbook (`docs/backfills.md`).
- Add a repeatable script to backfill `function_name` values in `gold.pce_state_annual`
  from BEA line metadata (`scripts/backfill_function_names.py`).
- Document backfill usage in setup/operability/readme docs.
- Execute a staging backfill for SAPCE3 and SAPCE4 and capture validation evidence.

## Acceptance Criteria

1. A documented, step-by-step backfill SOP exists with pre-flight, execution, validation, and recovery.
2. Backfill script supports dry-run and apply modes with explicit `run_id`.
3. Script records run metadata for auditability.
4. Staging SAPCE3 and SAPCE4 rows have non-empty `function_name` after apply.
5. Lint/tests pass after implementation.

## Constraints

- Keep implementation lightweight and idempotent.
- Avoid full historical fact re-ingest for metadata-only repairs.
- Do not change primary keys or serving view formulas.

## Non-Goals

- No scheduler/workflow orchestration changes in this feature.
- No expansion of dataset coverage beyond current SAPCE3/SAPCE4 needs.

## Rollout / Rollback

- Rollout: run dry-run then apply in staging, validate, then repeat in prod.
- Rollback: rerun script with corrected mapping using `--force`; no destructive table rewrites required.
