---
name: pce weighted rpp research
overview: Execute the state-level weighted RPP research from the existing serving view, producing reproducible query/analysis code plus the requested charts, CSV, and markdown summary under `research/outputs/`.
todos:
  - id: confirm-data-contract
    content: Confirm the target database environment, latest available year, category labels, and geography coverage in `serving.v_state_rpp_pce_weighted_annual` before analysis begins.
    status: pending
  - id: extract-analysis-data
    content: Build reproducible SQL/data extraction for national, exclusion-scenario, and five-year state trend datasets from the serving view.
    status: pending
  - id: generate-research-assets
    content: Create polished bar charts, per-state multi-line charts, and a consolidated CSV in `research/outputs/`.
    status: pending
  - id: write-findings
    content: Draft a markdown summary that explains the key movements, exclusion effects, and category-method caveats.
    status: pending
  - id: validate-results
    content: Cross-check weighted aggregation math, year coverage, and output completeness before handoff.
    status: pending
isProject: false
---

# Execute Weighted RPP Research

## Objective

Answer the questions in [research/prompts/pce-weighted-rpp-state-level.md](/home/john/tlg/macro-data-ingest/research/prompts/pce-weighted-rpp-state-level.md) using the existing serving view [src/macro_data_ingest/load/serving_views.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/serving_views.py), then save charts, a consolidated CSV, and a narrative summary under `research/outputs/`.

## Working Assumptions

- Use `serving.v_state_rpp_pce_weighted_annual` as the sole analytical source of truth.
- Treat prompt category `housing` as the view’s `Housing rents` category; call out that wording difference in the written summary.
- Follow the documented aggregation contract in [README.md](/home/john/tlg/macro-data-ingest/README.md):

```sql
SELECT
  year,
  category,
  SUM(weighted_rpp) / NULLIF(SUM(pce_share), 0) AS subset_price_level
FROM serving.v_state_rpp_pce_weighted_annual
WHERE state_abbrev <> 'CA'
GROUP BY year, category;
```

- For the full national result, use `SUM(weighted_rpp)` because `pce_share` already sums to 1 nationally.
- Validate whether the view includes DC and keep the geography rule consistent across all outputs.

## Files To Leverage

- [research/prompts/pce-weighted-rpp-state-level.md](/home/john/tlg/macro-data-ingest/research/prompts/pce-weighted-rpp-state-level.md): required questions and output contract.
- [src/macro_data_ingest/load/serving_views.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/serving_views.py): authoritative category mapping and `pce_share` / `weighted_rpp` logic.
- [README.md](/home/john/tlg/macro-data-ingest/README.md): correct subset renormalization rule and category notes.
- [tests/test_serving_views.py](/home/john/tlg/macro-data-ingest/tests/test_serving_views.py): quick check on expected category and weighting semantics.

## Execution Plan

1. Confirm data freshness and scope.

- Query the view for `MAX(year)`, distinct `category`, and geography count.
- Verify the five prompt states are present and determine whether DC is included.
- Record the latest year that will drive the comparison bar charts.

1. Extract the two analysis datasets.

- Build one query for latest-year category comparisons with three scenarios: `National`, `Without CA`, and `Without CA, NY, NJ, IL, CT`.
- Compute both the level and percent change versus national for each scenario/category pair.
- Build a second query for five-year history for `CA`, `NY`, `NJ`, `IL`, and `CT`, keeping one row per `year + state + category`.
- Export a single analysis CSV that contains the trend dataset and, if helpful, a `view_type` column so both result families can live in one file.

1. Produce reproducible research code.

- Add a small analysis script or notebook under `research/` that runs the SQL, shapes the data, and writes all outputs deterministically.
- Prefer a lightweight Python script with `pandas` and `matplotlib`/`seaborn` unless the repo already has a stronger plotting convention.
- Create a dedicated output folder such as `research/outputs/pce-weighted-rpp-state-level/` to keep PNG, CSV, and markdown artifacts grouped.

1. Generate the visuals.

- Create five bar charts, one per category, each with exactly three bars: `National`, `Without CA`, `Without CA, NY, NJ, IL, CT`.
- Create five state trend charts, one per state, with one line per RPP category over the latest five years.
- Keep titles and legends presentation-ready and consistent with the repo’s actual category labels.

1. Write the summary memo.

- Explain which categories move most when excluding California alone versus excluding the five high-cost states together.
- Highlight each target state’s five-year category changes and whether the movement is broad-based or concentrated in a few categories.
- Include methodological notes that `Housing rents` is proxied from the underlying PCE crosswalk and `Other services` is derived in the serving view.

1. Validate before handoff.

- Check that national rows satisfy `SUM(pce_share) ~= 1` by `year + category`.
- Recompute a few scenario values manually from the raw extracted rows to confirm the renormalized aggregation is correct.
- Verify the output set is complete: 5 bar PNGs, 5 line PNGs, 1 CSV, 1 markdown summary.

## Key Method Notes

The serving view already encodes the category crosswalk and weighting behavior, including:

```sql
CASE
    WHEN p.pce IS NULL THEN NULL
    ELSE p.pce / NULLIF(t.national_pce, 0)
END AS pce_share,
...
CASE
    WHEN rpp IS NULL OR pce_share IS NULL THEN NULL
    ELSE rpp * pce_share
END AS weighted_rpp
```

That means the exclusion scenarios should be computed by re-aggregating the included states, not by subtracting excluded states from a precomputed national index.

## Risks To Manage

- `Housing rents` in the view is not labeled simply `housing`, so the prompt language and output labels need alignment.
- `Other services` is a derived category, so the summary should avoid overstating its precision.
- If the target database has stale serving views, results may not match current repo semantics until views are refreshed.
- `research/outputs/` does not currently exist in the repo, so the execution work will need to create a clean output structure.

