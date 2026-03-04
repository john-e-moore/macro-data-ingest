# Backfill Runbook

This runbook defines the standard process for backfills in this repository.
Use it for both historical reloads and metadata repair tasks.

## Scope

Supported backfill types:
- **Data backfill**: rerun ingest/transform/load for one or more datasets/time windows.
- **Metadata backfill**: patch derived labels in Gold tables without re-ingesting source values.

Current metadata backfill utility:
- `python scripts/backfill_function_names.py`

## Safety Principles

- Always run in `staging` first, then `prod`.
- Use explicit `run_id` values for traceability.
- Keep Bronze immutable.
- Prefer idempotent updates and upserts.
- Capture validation evidence in PR/ExecPlan notes.

## Standard Pre-flight Checklist

1. Confirm active branch/PR scope and approval.
2. Confirm environment variables (`.env`) are loaded and correct.
3. Confirm DB connectivity and BEA API key presence.
4. Confirm target scope (dataset(s), year range, table list).
5. Run local checks:
   - `make lint PYTHON=.venv/bin/python`
   - `make test PYTHON=.venv/bin/python`

## Metadata Backfill: `function_name`

This is the recommended path when `gold.pce_state_annual` has missing or stale
`series_name` / `function_name` values and source numeric values are already correct.

### Dry run

```bash
python scripts/backfill_function_names.py \
  --env staging \
  --tables SAPCE4 \
  --run-id backfill-function-name-<yyyymmdd>-dryrun \
  --dry-run
```

### Apply

```bash
python scripts/backfill_function_names.py \
  --env staging \
  --tables SAPCE4 \
  --run-id backfill-function-name-<yyyymmdd>
```

Notes:
- Default behavior updates rows where either `series_name` or `function_name` is blank/null.
- Existing labels that still contain the legacy composite format (`[TABLE] Series: Function`)
  are normalized into `series_name` + `function_name`.
- Use `--force` to overwrite existing non-empty values from API metadata.
- The script records a `meta.ingest_runs` entry with `stage=backfill`.

## Validation Queries

Run after backfill:

```sql
SELECT bea_table_name, year,
       COUNT(*) AS rows,
       COUNT(*) FILTER (WHERE COALESCE(series_name, '') <> '') AS non_empty_series_name,
       COUNT(*) FILTER (WHERE COALESCE(function_name, '') <> '') AS non_empty_function_name
FROM gold.pce_state_annual
GROUP BY bea_table_name, year
ORDER BY bea_table_name, year DESC;
```

```sql
SELECT bea_table_name, line_code, series_name, function_name
FROM gold.pce_state_annual
WHERE bea_table_name = 'SAPCE4'
  AND line_code IN ('37')   -- Health
LIMIT 10;
```

Expected outcomes:
- `non_empty_series_name == rows` for intended scope.
- `non_empty_function_name == rows` for intended scope.
- Known SAPCE4 labels (for example line `37`) show expected values (`Total personal consumption expenditures` + `Health`).

## Recovery and Retry

- If BEA API errors occur, rerun the metadata backfill command; updates are idempotent.
- If DB connectivity fails mid-run, rerun with the same `run_id` or documented rerun ID policy.
- If wrong values were forced accidentally, rerun with corrected mapping and `--force`.

## Change Control Template

Document each backfill in your PR/plan:
- operator
- environment
- run_id
- command used
- scope (`tables`, dataset IDs, year range)
- validation query outputs
- follow-up actions
