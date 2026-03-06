# Feature Brief: Census State Population Source

## Scope

- Add US Census Bureau as a new source alongside BEA in the existing pipeline.
- Ingest one initial Census dataset: annual state population (`acs/acs1`, `B01003_001E`).
- Load a dedicated compatibility table for population and publish an initial per-capita serving view joined to BEA annual outputs.

## Acceptance Criteria

1. `config/datasets.yaml` includes an enabled Census state population dataset entry.
2. `mdi ingest|transform|load|run-all` supports mixed-source dataset specs and routes by `source`.
3. Census state population data lands in Bronze/Silver/Gold with idempotent reruns.
4. Postgres includes a population compatibility table keyed by `(state_fips, year)`.
5. A serving view exposes BEA annual values with population and per-capita calculation.
6. Tests and docs are updated to cover the new source and operator workflow.

## Constraints

- Keep existing BEA behavior unchanged.
- Preserve Bronze immutability and hash-based change detection.
- Preserve deterministic, idempotent Postgres upsert semantics.

## Non-Goals

- County-level ingestion in this change.
- Additional Census datasets beyond the initial state annual population table.
- Major refactor of existing BEA compatibility surfaces.

## Rollout / Rollback

- Rollout: merge to `main`, run `mdi run-all --env staging --dataset-id census_state_population` and validate serving view output.
- Rollback: disable Census dataset entry in `config/datasets.yaml` or revert branch/PR if needed.
