# Feature Brief: Add BEA SAGDP Table Group

## Scope

- Add state-level BEA `Regional` dataset entries for:
  - `SAGDP1` through `SAGDP9`
  - `SAGDP11`
  - `SASUMMARY`
- Keep implementation config-first so existing ingest/transform/load paths process new tables without code forks.
- Default all new entries to `bea_start_year: 2000` and `line_code: ALL` to capture full table coverage and available units/measures.

## Acceptance Criteria

1. Default dataset config includes enabled entries for all 11 requested table names.
2. Dataset example config includes the same 11 entries to keep operator setup aligned with defaults.
3. Tests cover dataset config parsing for the new SAGDP table list.
4. Staging pipeline run succeeds for each new dataset ID.
5. Docs reflect expanded BEA table coverage.

## Constraints

- Do not change BEA API client/query contracts or transform/load schema for this feature.
- Do not alter existing Census integration behavior.

## Non-Goals

- No serving-model redesign.
- No new non-BEA data sources.

## Rollout / Rollback

- Rollout: merge config updates and let scheduled `run-all` include the new dataset IDs.
- Rollback: set each SAGDP/SASUMMARY dataset entry to `enabled: false` (or remove) and rerun.
