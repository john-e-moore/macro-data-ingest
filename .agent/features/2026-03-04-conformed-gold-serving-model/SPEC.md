# Feature Brief: Conformed Gold Model + Serving OBT Contracts

## Scope

Implement a hybrid Postgres model where:

- `gold` stores conformed dimensions plus `fact_macro_observation` as the semantic system of record.
- `serving` exposes denormalized OBT-style views for analyst/API access.
- Existing BEA-centric contracts remain available for compatibility (`gold.pce_state_annual`, `serving.v_pce_state_yoy`).

## Acceptance Criteria

1. Load stage writes conformed `gold` dimensions/fact idempotently.
2. Serving views are generated from conformed tables (`obt_state_macro_annual_latest`, `v_macro_yoy`).
3. Existing BEA consumers can continue using `serving.v_pce_state_yoy`.
4. Repository docs and `.agent` guidance explicitly state the modeling strategy and schema contracts.
5. Unit tests cover new projection/model helpers.

## Constraints

- Keep implementation lightweight and avoid heavy migration frameworks.
- Preserve existing CLI workflows and dataset configuration behavior.
- No destructive changes to user data outside idempotent upserts.

## Non-goals

- Full multi-source ingestion implementation (BLS/Census/IRS remains roadmap work).
- Materialized view refresh orchestration/performance tuning beyond current needs.

## Rollout / Rollback

- Rollout: deploy code, run `mdi load` for a changed dataset, verify conformed and serving objects exist.
- Rollback: revert branch and redeploy previous loader implementation; compatibility tables/views remain available.
