# Remove SAPCE3 From Ingest and Gold Table

## Context

`SAPCE3` duplicates the same top-level "Personal consumption expenditures" signal carried by
`SAPCE4` in this project and creates duplicate-risk in `gold.pce_state_annual` when analysts
forget to filter by `bea_table_name`.

## Scope

- Remove `SAPCE3` from default ingest configuration and examples.
- Make `SAPCE4` the default legacy fallback table name.
- Update runbook/setup docs to avoid `SAPCE3` in normal operations.
- Execute database cleanup to remove existing `SAPCE3` rows from `gold.pce_state_annual`.

## Non-Goals

- No change to table schema.
- No change to SAPCE4 transform semantics.
- No historical rewrite of prior run metadata.

## Acceptance Criteria

1. Default dataset config ingests only `SAPCE4`.
2. Legacy env fallback defaults to `BEA_TABLE_NAME=SAPCE4`.
3. Documentation examples use `SAPCE4` only for current operations.
4. Database contains zero rows where `bea_table_name = 'SAPCE3'` after cleanup.

## Rollback

- Re-add `SAPCE3` dataset entry in `config/datasets.yaml` and `config/datasets.example.yaml`.
- Restore `BEA_TABLE_NAME` default to previous value.
- Re-ingest SAPCE3 explicitly if needed.
