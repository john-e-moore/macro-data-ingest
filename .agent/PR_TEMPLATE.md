# Pull Request Template

Use this template for feature and substantial refactor PRs.
Keep it concise, evidence-based, and tied to repo requirements.

## Summary

- What behavior changed for users/operators?
- Why was this needed now?

## Links

- Branch: `<feature-branch-name>`
- Feature brief: `.agent/features/<YYYY-MM-DD>-<feature-name>/SPEC.md` (or `N/A`)
- ExecPlan entry: `.agent/PLANS.md` (`<plan title>`)

## Scope and Non-Goals

- In scope:
- Not in scope:

## Acceptance Criteria Mapping

- [ ] Criterion 1:
- [ ] Criterion 2:
- [ ] Criterion 3:

## Validation Evidence

- [ ] Lint/format checks passed
- [ ] Unit tests passed
- [ ] Smoke/E2E path verified (or documented why not run)
- [ ] Idempotent re-run behavior verified

Commands and key outputs:

```text
# Example:
make lint
make test
python -m macro_data_ingest.cli run --env staging --smoke
```

## Data and Contract Impact

- Schema/table/view changes:
- Partitioning/storage convention changes:
- Backward compatibility notes:

## Operations and Rollback

- Runbook/docs updated:
- Alerting/observability impact:
- Rollback or recovery steps:

## Security and Cost Checks

- [ ] No secrets committed
- [ ] IAM/data access still least privilege
- [ ] Cost controls considered (lifecycle, reload behavior, instance sizing)

## Risks and Follow-Ups

- Known risks:
- Follow-up tasks (if any):
