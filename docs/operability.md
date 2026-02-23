# Operability Guide

## Logging Strategy

- Emit structured JSON logs by default.
- Include `run_id`, `env`, `stage`, and `component` fields.
- Use conventional levels:
  - `INFO` for lifecycle and checkpoints
  - `WARNING` for recoverable anomalies
  - `ERROR` for failed operations

Primary sinks:
- stdout/stderr in local and GitHub Actions runs
- CloudWatch log groups for environment-level auditability

## Data Quality Checks

Minimum checks by stage:
- Non-null key checks on required business keys
- Expected schema column presence/type checks
- Row count sanity checks between stages
- Uniqueness checks for designated primary keys

Failure behavior:
- Mark run as failed
- Persist failed check metadata where possible
- Stop downstream stages when severity is blocking

## Alerting

Baseline:
- GitHub Actions failure notifications

Optional:
- SNS publication on run failures (when topic ARN is configured)
- Future Slack/email bridge through SNS subscriptions

## Backfill Strategy

Guidelines:
- Backfill via explicit date/category parameters
- Process in deterministic batches by partition
- Preserve Bronze immutability; regenerate Silver/Gold deterministically
- Use idempotent upserts for Postgres targets

Recommended controls:
- Limit batch size by date interval
- Use resumable checkpoints to continue partial backfills

## Cost Controls

- Use S3 lifecycle policies for aged logs and stale transient artifacts.
- Keep Bronze retention policy explicit and environment-specific.
- Prefer small RDS instance classes initially.
- Avoid full daily reloads by source-change detection.
- Separate staging and prod costs through environment prefixes/tags.

## Failure Recovery

If a run fails:
1. Inspect run metadata and logs by `run_id`.
2. Determine failing stage and whether outputs are partial.
3. Re-run failed stage with same `run_id` or a documented rerun policy.
4. Confirm no duplicate records in Postgres and no malformed partitions in S3.
