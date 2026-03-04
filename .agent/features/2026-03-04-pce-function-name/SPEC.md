# Feature Brief: Add `function_name` to Gold PCE table

## Context

The `gold.pce_state_annual` table currently stores `series_code` and `line_code`, but not a human-readable function label from BEA metadata. Analysts need the label in-table for direct SQL consumption.

## Scope

- Fetch BEA `LineCode` descriptions from API metadata.
- Carry the description through ingest payload normalization.
- Add `function_name` to Silver and Gold contracts.
- Persist `function_name` in Postgres table `gold.pce_state_annual`.
- Ensure column appears between `series_code` and `pce_value` in table DDL and Gold frame ordering.
- Add/update tests for client, ingest, Silver, and Gold behavior.

## Acceptance Criteria

1. BEA line metadata includes a `line_code -> function_name` mapping and is applied to ingested rows.
2. Silver and Gold records include a non-null `function_name` field (empty string fallback allowed when metadata is unavailable).
3. Postgres `gold.pce_state_annual` has `function_name` between `series_code` and `pce_value`.
4. Existing tables migrate safely (`ADD COLUMN IF NOT EXISTS`, backfill nulls, enforce `NOT NULL`).
5. Unit tests pass for updated contracts and behavior.

## Constraints

- Keep existing primary key and idempotent upsert semantics unchanged.
- Avoid introducing new runtime dependencies.
- Preserve backward-compatible behavior for older payload shapes that may omit line description fields.

## Non-Goals

- No change to derived view formulas (`serving.v_pce_state_yoy`).
- No expansion of dataset scope beyond existing configured BEA tables.

## Rollout / Rollback

- Rollout: deploy code, run load stage to apply table migration, then validate schema and sample values.
- Rollback: revert code; if needed, leave `function_name` column in place since additive schema changes are backward-compatible for readers that ignore it.
