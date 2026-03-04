# AGENTS.md

This file defines how coding agents should work in this repository.
It complements `README.md` and `docs/` by focusing on execution behavior.

## Project Context

- Repository purpose: build and operate a lightweight BEA PCE-by-state ingestion and warehousing pipeline.
- Primary architecture: S3 Bronze/Silver/Gold plus Postgres (RDS) serving tables/views.
- Environments: `staging` and `prod`.
- Source of truth for requirements: `.agent/SPEC.md`.

## Instruction Priority

When instructions conflict, use this precedence:

1. Direct user request in the active session.
2. This file (`.agent/AGENTS.md`).
3. Project docs (`README.md`, `docs/*`, `.agent/SPEC.md`).
4. Default toolchain conventions.

If required information is missing, make a safe, explicit assumption and document it in your plan/worklog.

## Working Agreement

- Keep the implementation lightweight, practical, and reproducible.
- Prefer simple Python tooling and minimal dependencies.
- Preserve idempotency across ingestion, transforms, and Postgres loads.
- Do not store secrets in code, committed files, or logs.
- Treat observability, data quality checks, and failure handling as first-class requirements.

## Non-Negotiable Engineering Constraints

- Batch processing only (no streaming for this phase).
- GitHub Actions is the scheduler/orchestrator for daily runs.
- Bronze data is immutable raw payload storage.
- Silver/Gold schemas must be stable and analytics-friendly.
- Loading into Postgres must be deterministic and safe to re-run.
- AWS provisioning must be repeatable and support both `staging` and `prod`.

## Required Delivery Behavior

For any non-trivial implementation/refactor, the agent should:

1. Read `.agent/SPEC.md` before coding.
2. Create a feature-scoped brief under `.agent/features/` when work introduces net-new behavior (see Workflow section below).
3. Create or update an ExecPlan in `.agent/PLANS.md` (see section below).
4. Implement in small, testable increments.
5. Run relevant checks/tests and record outcomes.
6. Update docs impacted by behavioral changes.

Do not stop at partial implementation unless blocked by missing credentials, external access, or explicit user direction.

## Feature Workflow (Branch to PR)

Use the following default workflow for substantial feature work:

1. Create a feature branch from `main`.
2. Create a feature brief at `.agent/features/<YYYY-MM-DD>-<feature-name>/SPEC.md`.
3. Create or update an ExecPlan in `.agent/PLANS.md` and link to the feature brief.
4. Implement in reviewable increments and keep the ExecPlan progress/decisions current.
5. Run lint/tests/smoke checks and capture concise evidence.
6. Open a PR using `.agent/PR_TEMPLATE.md`.

Feature briefs should be concise and should not duplicate repository-wide requirements from `.agent/SPEC.md`.
They should define scope, acceptance criteria, constraints, non-goals, and rollout/rollback notes specific to that feature.

## ExecPlans

When writing complex features or significant refactors, use an ExecPlan (as described in `.agent/PLANS.md`) from design through validation.

Use an ExecPlan by default for work that:

- touches multiple pipeline stages (ingest + transform + load),
- changes storage schema or partitioning,
- alters provisioning/IAM/network/RDS setup,
- changes CI/CD workflows or reliability guarantees.

## Coding and Repository Conventions

- Prefer explicit module boundaries:
  - API client
  - Bronze writer
  - Silver transforms
  - Gold modeling
  - Postgres loader
  - run metadata/checkpoints
  - provisioning script(s)
- Use typed interfaces where practical.
- Keep logs structured and include a `run_id`.
- Record lineage metadata (source params, hashes, partitions, row counts) per run.
- Favor deterministic file and partition naming.

## Data Modeling Strategy (Postgres)

Agents should treat this as the default modeling contract unless a user request overrides it:

- `gold` is the semantic system of record and must maintain conformed dimensions + fact:
  - `gold.dim_source`
  - `gold.dim_geo`
  - `gold.dim_period`
  - `gold.dim_series`
  - `gold.dim_vintage`
  - `gold.fact_macro_observation`
- `serving` is the consumer contract layer and should expose denormalized OBT/views for common
  query paths (`serving.obt_state_macro_annual_latest`, YoY/MoM derivatives).
- Legacy BEA-specific tables/views may be kept for backward compatibility but should be treated as
  compatibility surfaces, not the long-term semantic core.
- Cross-source joins (BEA/BLS/Census/IRS) should happen through conformed dimensions/fact and only
  then be published to `serving`.

## Validation Expectations

Before claiming completion, validate with:

- unit tests for API client/helpers/transforms,
- at least one smoke path for tiny end-to-end pull,
- lint/format checks,
- evidence that idempotent re-run behavior is preserved.

If environment constraints prevent full validation, report exactly what was run, what was not run, and why.

## Documentation Expectations

Update documentation whenever behavior changes:

- setup or environment variable requirements,
- architecture assumptions (Bronze/Silver/Gold, partitioning, serving model),
- operations runbooks (alerts, backfills, failure handling),
- CI/CD and scheduling behavior.

## Security and Cost Guardrails

- Apply least-privilege IAM permissions.
- Keep S3 encryption + public access blocking enabled.
- Use conservative RDS sizing guidance.
- Prefer change detection over full daily reloads.
- Include lifecycle/retention guidance to control storage cost.

## Completion Checklist (for agents)

- Requirements traced back to `.agent/SPEC.md`.
- Plan updated in `.agent/PLANS.md` if scope is substantial.
- Code, tests, and docs aligned.
- No secrets leaked.
- Validation results communicated clearly.
