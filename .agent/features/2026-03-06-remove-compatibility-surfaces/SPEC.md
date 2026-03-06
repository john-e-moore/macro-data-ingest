# Remove Postgres Compatibility Surfaces

## Scope

Simplify Postgres contracts before frontend integration by removing legacy compatibility tables/views
and dual-write logic, while preserving conformed-load behavior and serving outputs backed by
`gold.fact_macro_observation`.

Objects removed:
- `gold.pce_state_annual`
- `gold.pce_state_monthly`
- `gold.population_state_annual`
- `gold.state_gov_finance_annual`
- `serving.v_pce_state_yoy`

## Acceptance Criteria

- `mdi load` no longer writes compatibility tables.
- Load creates/refreshes conformed tables and serving views only.
- Legacy objects are dropped idempotently during loader setup.
- `mdi run-all` remains idempotent (no duplicate growth in conformed fact keys).
- Dataset config and tests no longer depend on `target_table`.
- Docs describe conformed + serving contracts only.

## Constraints

- Keep existing S3 Bronze/Silver/Gold data flow unchanged.
- Keep `meta.ingest_runs` lineage behavior intact.
- Preserve deterministic upsert semantics in conformed dimensions/fact.

## Non-goals

- No redesign of BEA/Census source semantics.
- No materialized-view optimization work in this change.
