# Feature Brief: Add BEA SARPP and SARPI Series

## Scope

- Add state-level BEA `Regional` series configs for:
  - `SARPP` (regional price parities by state)
  - `SARPI` (real personal income and real PCE by state)
- Keep integration config-first through `config/datasets.yaml` and `config/datasets.example.yaml`.
- Preserve existing ingest/transform/load routing and table contracts.

## Acceptance Criteria

1. Daily/default datasets include enabled `SARPP` and `SARPI` entries with `line_code: ALL`.
2. New series request data from year `2000` through current year by default.
3. Tests cover dataset-spec parsing for the new table entries.
4. Staging ingest runs successfully for both new dataset IDs.
5. Docs/spec reflect new coverage and the default start-year policy.

## Constraints

- Do not add Census joins or cross-source merge behavior in this feature.
- Do not change existing monthly SAPCE4 staging behavior.

## Non-Goals

- No serving-view redesign.
- No additional transform/load schema changes beyond what current BEA routing already supports.

## Rollout / Rollback

- Rollout: merge and let scheduled `run-all` process the new dataset IDs.
- Rollback: set `enabled: false` (or remove entries) for `SARPP`/`SARPI` in dataset config and rerun.
