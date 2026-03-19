---
name: weighted rpp update
overview: Update the weighted RPP serving view so it exposes `pce_share` and redefines `weighted_rpp` as `rpp * pce_share`, then apply the revised view SQL directly to the staging database.
todos:
  - id: update-view-sql
    content: Revise `v_state_rpp_pce_weighted_annual` SQL to compute `pce_share` and redefine `weighted_rpp`.
    status: completed
  - id: update-tests
    content: Adjust serving view SQL tests for the new share-based formula and output column.
    status: completed
  - id: validate-local
    content: Run targeted validation for generated SQL before database execution.
    status: completed
  - id: apply-staging-sql
    content: Execute the revised view SQL directly against `staging` and verify the updated view contract.
    status: completed
isProject: false
---

# Update Weighted RPP View

## Scope

Adjust `serving.v_state_rpp_pce_weighted_annual` so:

- `pce_share` is each state's share of national PCE for the same `year` and `category_key`.
- `weighted_rpp` becomes `rpp * pce_share`.
- The revised view is applied directly to the `staging` database.

## Files To Change

- [src/macro_data_ingest/load/serving_views.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/serving_views.py)
- [tests/test_serving_views.py](/home/john/tlg/macro-data-ingest/tests/test_serving_views.py)
- Optionally, if we want docs to match the contract change:
- [README.md](/home/john/tlg/macro-data-ingest/README.md)
- [docs/architecture.md](/home/john/tlg/macro-data-ingest/docs/architecture.md)

## Implementation Plan

1. Update the SQL generator in [src/macro_data_ingest/load/serving_views.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/serving_views.py).
  - Keep the existing `rpp_rows` and category mapping.
  - Refactor the current `pce_weight_rows` CTE into a reusable PCE-by-state result, then add a second step that computes national category totals by `year` and `category_key`.
  - Add `pce_share` with a null-safe formula like `state_pce / national_pce` using `NULLIF(national_pce, 0)`.
  - Change `weighted_rpp` from `r.rpp * p.pce` to `r.rpp * pce_share`.
  - Preserve existing metadata columns unless there is a strong reason to rename or drop them.
2. Extend SQL-generation tests in [tests/test_serving_views.py](/home/john/tlg/macro-data-ingest/tests/test_serving_views.py).
  - Replace the current assertion that expects `ELSE r.rpp * p.pce`.
  - Add assertions that the generated SQL includes `pce_share` and a national-total share calculation.
  - Keep existing coverage for the SARPP/SAPCE crosswalk and derived `other_services` logic.
3. Validate locally before touching the database.
  - Run the targeted test file for serving-view SQL generation.
  - If needed, inspect the rendered SQL string to confirm the new column order and formula.
4. Apply the view change directly to `staging`.
  - Use the repo’s config/env to connect to Postgres.
  - Execute the updated DDL for `serving.v_state_rpp_pce_weighted_annual` directly against `staging` rather than relying on `mdi run-all`, since `run-all` can skip load when upstream ingest is unchanged.
  - If the script in [src/macro_data_ingest/load/serving_views.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/serving_views.py) still emits the full serving-view bundle, run it in a controlled way and verify all recreated views succeed.
5. Verify the database result in `staging`.
  - Confirm the view now exposes `pce_share`.
  - Spot-check that, for a given `year` and category, `SUM(pce_share)` across states is approximately `1`.
  - Spot-check that `weighted_rpp = rpp * pce_share`.

## Key Existing Logic

Current `weighted_rpp` is defined in [src/macro_data_ingest/load/serving_views.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/serving_views.py) as:

```207:210:src/macro_data_ingest/load/serving_views.py
        CASE
            WHEN r.rpp IS NULL OR p.pce IS NULL THEN NULL
            ELSE r.rpp * p.pce
        END AS weighted_rpp,
```

This will be replaced with a share-based computation after introducing a national-total step.

## Validation Queries

Use checks like:

```sql
SELECT year, category, ROUND(SUM(pce_share)::numeric, 8) AS share_sum
FROM serving.v_state_rpp_pce_weighted_annual
GROUP BY year, category
ORDER BY year DESC, category;
```

```sql
SELECT year, state_fips, category, rpp, pce_share, weighted_rpp,
       (rpp * pce_share) AS expected_weighted_rpp
FROM serving.v_state_rpp_pce_weighted_annual
ORDER BY year DESC, state_fips, category
LIMIT 25;
```

## Risks

- The current SQL builder recreates multiple serving views in one script, so a direct apply may refresh more than just this one view.
- Downstream consumers expecting the old interpretation of `weighted_rpp` may need to be notified.
- If docs or analyst queries describe `SUM(weighted_rpp) / SUM(pce)`, they should be updated because the new weighting contract changes that interpretation.

