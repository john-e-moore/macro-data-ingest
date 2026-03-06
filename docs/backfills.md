# Backfill Runbook

This runbook defines the standard process for historical backfills in this repository.
Backfills should use the normal dataset pipeline path so output contracts remain consistent.

## Scope

Supported backfill type:
- **Data backfill**: rerun ingest/transform/load for one or more dataset IDs and time windows.

## Safety Principles

- Always run in `staging` first, then `prod`.
- Use explicit `run_id` values for traceability.
- Keep Bronze immutable.
- Prefer idempotent updates and upserts.
- Capture validation evidence in PR/ExecPlan notes.

## Standard Pre-flight Checklist

1. Confirm active branch/PR scope and approval.
2. Confirm environment variables (`.env`) are loaded and correct.
3. Confirm DB connectivity and source API key presence.
4. Confirm target scope (dataset IDs and year range).
5. Run local checks:
   - `make lint PYTHON=.venv/bin/python`
   - `make test PYTHON=.venv/bin/python`

## Standard Backfill Commands

Use the same CLI stages as daily runs. For most cases, run all stages together:

```bash
mdi run-all --env staging --run-id backfill-<dataset>-<yyyymmdd> --dataset-id <dataset_id>
```

For controlled reruns, execute stages separately:

```bash
mdi ingest --env staging --run-id backfill-<dataset>-<yyyymmdd> --dataset-id <dataset_id>
mdi transform --env staging --run-id backfill-<dataset>-<yyyymmdd> --dataset-id <dataset_id>
mdi load --env staging --run-id backfill-<dataset>-<yyyymmdd> --dataset-id <dataset_id>
```

## Validation Queries

Run after a backfill completes:

```sql
SELECT COUNT(*) AS rows
FROM gold.fact_macro_observation;
```

```sql
SELECT source_name, dataset_id, COUNT(*) AS rows
FROM serving.obt_state_macro_annual_latest
GROUP BY source_name, dataset_id
ORDER BY source_name, dataset_id;
```

Expected outcomes:
- target dataset appears in `serving.obt_state_macro_annual_latest` with non-zero rows when source coverage exists;
- rerunning the same dataset does not cause duplicate growth in `gold.fact_macro_observation`.

## Recovery and Retry

- If source/API errors occur, rerun the same command with a new `run_id`.
- If DB connectivity fails mid-run, rerun `mdi load`; conformed upserts are idempotent.
- If a failure occurs after ingest but before load, rerun `mdi transform` and `mdi load` with documented run IDs.

## Change Control Template

Document each backfill in your PR/plan:
- operator
- environment
- run_id
- command used
- scope (dataset IDs, year range)
- validation query outputs
- follow-up actions
