# Feature Brief: Hierarchy-capable BEA Series Dimension

## Context

Some BEA tables (for example SAPCE1) use tiered line descriptions where one series belongs to a multi-level category path (Goods -> Durable goods -> ...). The current model stores only flat `series_name` and `function_name`, which is not sufficient as the canonical semantic structure.

## Scope

- Preserve backward-compatible `series_name` and `function_name` fields.
- Capture canonical raw BEA line description and full hierarchy path in Silver/Gold/conformed flow.
- Extend `gold.dim_series` with raw/parsed metadata fields.
- Add hierarchy tables:
  - `gold.dim_series_node`
  - `gold.bridge_series_node`
- Populate hierarchy tables idempotently during conformed load.
- Update tests and architecture docs.

## Acceptance Criteria

1. Tiered BEA labels parse into a full hierarchy path in transforms.
2. Conformed load persists hierarchy metadata in `gold.dim_series`.
3. Hierarchy nodes/bridge rows are materialized and idempotently refreshed per source/table.
4. Existing SAPCE4-style labels remain backward compatible in `series_name` and `function_name`.
5. Unit tests pass for parser and transform contract updates.

## Constraints

- Keep existing fact PKs and serving contracts intact.
- Keep implementation lightweight with no new runtime dependencies.
- Ensure reruns do not duplicate hierarchy bridge/node rows.

## Non-Goals

- No SAPCE1 dataset ingest in this branch.
- No changes to scheduler or workflow orchestration.
- No redesign of fact grain or existing serving YoY formulas.

## Rollout / Rollback

- Rollout: deploy code, run load stage, validate `dim_series`, `dim_series_node`, and `bridge_series_node`.
- Rollback: revert code; additive schema objects can remain without impacting existing consumers.
