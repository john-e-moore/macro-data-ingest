# PLANS.md

This file defines the execution-plan standard for this repository.
An execution plan in this repo is called an **ExecPlan**.

ExecPlans are used to design and drive multi-step work end-to-end with clear validation.
They are living documents and must remain current as implementation evolves.

## When To Use An ExecPlan

Create/update an ExecPlan before implementation when work is complex, high-risk, or cross-cutting, especially when it:

- spans multiple pipeline layers (ingest, Bronze/Silver/Gold transforms, load),
- changes data contracts or partitioning conventions,
- modifies provisioning/IAM/network/RDS setup,
- affects observability, quality checks, or run metadata,
- changes CI workflows, schedules, or deployment behavior.

For small local fixes (single-file bug fix, typo, minor test update), an ExecPlan is optional.

## Relationship to Feature Briefs

`.agent/SPEC.md` remains the repository-level source of truth for baseline requirements.

For substantial feature work, create a feature brief at:

- `.agent/features/<YYYY-MM-DD>-<feature-name>/SPEC.md`

Then create/update an ExecPlan in this file that references the feature brief path near the top (`Purpose / Big Picture` or `Context and Orientation`).

Feature briefs should stay focused on feature-local requirements (scope, acceptance criteria, non-goals, constraints, rollout/rollback) and should not duplicate the full repo baseline spec.

## Core Principles

Every ExecPlan must be:

- **Self-contained**: a new contributor can execute it using only the plan + repo state.
- **Outcome-focused**: defines what behavior users/operators can observe at the end.
- **Executable**: includes concrete commands and expected evidence.
- **Living**: updated as decisions, discoveries, and progress change.
- **Safe and repeatable**: steps are idempotent where possible, with recovery notes.

Define terms of art in plain language (for example: "Bronze", "upsert", "lineage", "checkpoint").

## Required Sections (Mandatory)

Each ExecPlan must include all sections below:

1. Purpose / Big Picture
2. Progress
3. Surprises & Discoveries
4. Decision Log
5. Outcomes & Retrospective
6. Context and Orientation
7. Plan of Work
8. Concrete Steps
9. Validation and Acceptance
10. Idempotence and Recovery
11. Artifacts and Notes
12. Interfaces and Dependencies

Additionally, each ExecPlan should include a short "Links" line near the top listing:

- feature branch name,
- feature brief path (if applicable),
- PR URL (once opened).

`Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` are mandatory living sections and must be kept up to date during execution.

## Formatting Rules

- Write in clear prose; use short lists only when they improve readability.
- Use repository-relative paths for all file references.
- Include exact commands with working directory context.
- Include concise evidence snippets (test output, command output, key logs).
- Keep plans in markdown format and easy to diff.

If an ExecPlan is the entire file content, no outer code fence is required.

## Repository-Specific Acceptance Bar

For this project, an ExecPlan is only complete when the defined scope demonstrates:

- behavior works end-to-end (not just compile-level changes),
- data quality checks and failure modes are handled appropriately,
- idempotent re-runs do not produce duplicates or drift,
- run metadata/lineage is persisted and inspectable,
- required docs are updated for operators.

When AWS/RDS credentials are unavailable, provide a local or mocked validation path plus explicit production validation steps.

## ExecPlan Template

Copy this template for each substantial project task.

---

# <ExecPlan title>

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Describe the user/operator-visible outcome. Explain what becomes possible after this change and how to observe it.

## Progress

- [ ] (YYYY-MM-DD HH:MMZ) Initial planning completed.
- [ ] Implementation milestone 1 completed.
- [ ] Validation and documentation updates completed.

## Surprises & Discoveries

- Observation: <unexpected behavior, risk, or insight>
  Evidence: <short command output, log line, or test signal>

## Decision Log

- Decision: <what was decided>
  Rationale: <why this choice>
  Date/Author: <YYYY-MM-DD, name/agent>

## Outcomes & Retrospective

Summarize achieved outcomes, remaining gaps, and lessons learned. Compare results against the original purpose.

## Context and Orientation

Explain current relevant repository state for a newcomer. Name key files and how they connect, for example:

- `src/ingest/*` for API ingestion and raw writes,
- `src/transforms/*` for Silver/Gold transforms,
- `src/load/*` for Postgres upsert logic,
- `scripts/provision_aws.py` for environment provisioning,
- `.github/workflows/*` for CI and scheduling.

## Plan of Work

Describe the sequence of edits and why. Be explicit about paths, functions/modules, and intended behavior.

## Concrete Steps

List exact commands with working directory. Example:

    cd /path/to/repo
    python -m pytest -q
    python -m <project_cli> ingest --env staging --smoke

Include expected outcomes for each command.

## Validation and Acceptance

Define observable acceptance criteria. Prefer behavior checks, for example:

- running ingest writes Bronze payload and run manifest,
- transform produces expected Silver/Gold partitions,
- load performs idempotent upsert into Postgres tables/views,
- tests pass and smoke run succeeds.

## Idempotence and Recovery

Describe safe re-run behavior and rollback/recovery steps for partial failures.

## Artifacts and Notes

Include concise evidence snippets (logs, short diffs, query output) that prove success.

## Interfaces and Dependencies

Name libraries/services/modules used and any required interfaces or function signatures that must exist at completion.

---

# Standardized Backfill SOP and Function Name Metadata Repair

This ExecPlan is a living document and follows `.agent/PLANS.md`.

Links: branch `feature/backfill-runbook-function-name`; feature brief `.agent/features/2026-03-04-backfill-runbook-function-name/SPEC.md`; PR `TBD`.

## Purpose / Big Picture

Establish a standardized, repeatable backfill process and execute a targeted metadata backfill so
`gold.pce_state_annual.function_name` is populated for existing SAPCE3/SAPCE4 rows without
re-ingesting historical fact values. The observable outcome is complete function labels plus an
operator-ready runbook and utility for future backfills.

## Progress

- [x] (2026-03-04 00:00Z) Initial planning completed.
- [x] Implementation completed.
- [x] Validation and documentation updates completed.

## Surprises & Discoveries

- Observation: SAPCE4 line-code metadata from BEA is complete, but existing DB rows were loaded before enrichment.
  Evidence: `mapping_size=134` for SAPCE4 with non-empty labels while DB had `non_empty_fn=0`.
- Observation: historical SAPCE3 rows remained blank while 2024 test rows were populated.
  Evidence: grouped table/year query showed SAPCE3 `2024` non-empty and `2000-2023` empty.

## Decision Log

- Decision: implement metadata-only backfill script for function labels instead of full historical re-ingest.
  Rationale: avoids BEA `429` risk and updates only derived label fields while preserving numeric facts.
  Date/Author: 2026-03-04, codex agent

## Outcomes & Retrospective

Implemented and validated in staging. The new metadata backfill utility populated all missing
`function_name` values for both SAPCE3 and SAPCE4 without re-ingesting full historical data.
Runbook documentation is now explicit and command-driven, with dry-run/apply/validation steps.

## Context and Orientation

Relevant paths:
- `docs/operability.md` (policy-level runbook guidance),
- `src/macro_data_ingest/ingest/bea_client.py` (line-code descriptions),
- `src/macro_data_ingest/load/postgres_loader.py` (DB connectivity conventions),
- `gold.pce_state_annual` in Postgres where backfill applies.

## Plan of Work

1. Add `docs/backfills.md` with standardized SOP.
2. Add `scripts/backfill_function_names.py` with dry-run/apply, explicit run IDs, and audit record writes.
3. Update docs references in `README.md`, `docs/setup.md`, and `docs/operability.md`.
4. Run lint/tests.
5. Execute dry-run then apply for SAPCE3/SAPCE4 in staging and validate row-level outcomes.
6. Update plan evidence and open PR.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    .venv/bin/python scripts/backfill_function_names.py --env staging --tables SAPCE3,SAPCE4 --run-id backfill-function-name-20260304-dryrun --dry-run
    .venv/bin/python scripts/backfill_function_names.py --env staging --tables SAPCE3,SAPCE4 --run-id backfill-function-name-20260304

Expected outcomes:
- docs and script in place;
- tests/lint pass;
- SAPCE3 and SAPCE4 function labels become non-empty in `gold.pce_state_annual`.

## Validation and Acceptance

Acceptance checks:
- runbook exists and is actionable.
- script supports `--dry-run`, `--run-id`, and `--force`.
- `meta.ingest_runs` records backfill stage metadata.
- grouped table/year query shows non-empty `function_name` coverage for SAPCE3/SAPCE4.

## Idempotence and Recovery

Idempotence:
- default behavior updates only blank labels;
- reruns are safe and should produce zero updates once complete.

Recovery:
- if interrupted, rerun the same command;
- if values need correction, rerun with `--force`.

## Artifacts and Notes

Artifacts:
- `.agent/features/2026-03-04-backfill-runbook-function-name/SPEC.md`
- `docs/backfills.md`
- `scripts/backfill_function_names.py`
- `docs/operability.md`
- `docs/setup.md`
- `README.md`
- `tests/test_backfill_function_names.py`

Evidence snippets:
- `python scripts/backfill_function_names.py --env staging --tables SAPCE3,SAPCE4 --run-id backfill-function-name-20260304-dryrun --dry-run`
  - SAPCE3 `empty_before=1224`, SAPCE4 `empty_before=169575`
- `python scripts/backfill_function_names.py --env staging --tables SAPCE3,SAPCE4 --run-id backfill-function-name-20260304`
  - SAPCE3 `rows_updated=1224`, SAPCE4 `rows_updated=169575`, both `empty_after=0`
- Validation query:
  - SAPCE3 coverage `1275/1275` non-empty
  - SAPCE4 coverage `169575/169575` non-empty
  - SAPCE4 line code `37` label resolved to `Health`

## Interfaces and Dependencies

Interfaces:
- `python scripts/backfill_function_names.py --env <staging|prod> --tables SAPCE3,SAPCE4 --run-id <id> [--dry-run] [--force]`

Dependencies:
- BEA API metadata endpoint (`GetParameterValuesFiltered`)
- Postgres access to `gold.pce_state_annual` and `meta.ingest_runs`

---

## Optional: Active ExecPlan Index

Use this section to track in-flight plans:

- `<date> - <plan name> - status: planned|in_progress|blocked|done - owner: <name>`

Keep this index current when running long tasks.

---

# Initial Documentation and Scaffolding

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Create a complete project scaffold that a new contributor can run locally and extend through vertical slices without reworking structure. The immediate observable outcome is a documented repository with a runnable CLI skeleton, workflow skeletons, and module boundaries aligned to the target architecture.

## Progress

- [x] (2026-02-23 00:00Z) Initial planning completed.
- [x] Documentation scaffold completed.
- [x] Project/code/workflow skeleton completed.
- [x] Validation and retrospective updates completed.

## Surprises & Discoveries

- Observation: The repository started nearly empty and required full initial structure creation.
  Evidence: root contained only `README.md` and `TODO.txt` plus hidden config files.

## Decision Log

- Decision: Use Python package + CLI scaffold with explicit ingest/transform/load module boundaries.
  Rationale: Matches requirements while keeping implementation lightweight and ready for incremental vertical slices.
  Date/Author: 2026-02-23, codex agent

## Outcomes & Retrospective

Scaffolded docs, config surfaces, workflow files, package layout, and tests. Functional logic is intentionally deferred to vertical slices to preserve reviewable increments. Validation ran successfully with `make lint test PYTHON=.venv/bin/python` (ruff pass, 2 tests passed).

## Context and Orientation

Key files now include:
- `src/macro_data_ingest/cli.py` for command entrypoints,
- `src/macro_data_ingest/ingest/*` for BEA and Bronze boundaries,
- `src/macro_data_ingest/transforms/*` for Silver/Gold boundaries,
- `src/macro_data_ingest/load/*` for Postgres boundaries,
- `scripts/provision_aws.py` for environment provisioning interface,
- `.github/workflows/*` for CI and daily scheduling,
- `docs/*` for setup, architecture, operability, and roadmap.

## Plan of Work

1. Establish docs for setup, architecture, requirements, operability, and roadmap.
2. Create package and CLI scaffolding aligned with required module boundaries.
3. Add configuration, logging, metadata helpers, and stub stage modules.
4. Add CI and daily pipeline workflow skeletons.
5. Add minimal tests validating scaffold wiring and run local checks.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    pip install -e .[dev]
    make lint
    make test

Expected outcome: lint and tests pass for scaffolded code.

## Validation and Acceptance

Acceptance checks for this scaffold scope:
- Documentation exists for setup/architecture/spec/operability/roadmap.
- CLI command surface exists with staged commands.
- Required module boundaries exist and import cleanly.
- CI and schedule workflows are present and syntactically valid.
- Local lint/tests pass (pending environment execution).

## Idempotence and Recovery

Scaffold files are deterministic and safe to re-apply. If adjustment is needed, edits should be incremental to preserve history and avoid hidden behavior changes.

## Artifacts and Notes

Primary artifacts:
- `README.md`
- `docs/*.md`
- `pyproject.toml`
- `src/macro_data_ingest/**`
- `scripts/provision_aws.py`
- `.github/workflows/*.yml`

## Interfaces and Dependencies

Defined interfaces (stub level):
- CLI: `mdi ingest|transform|load|run-all`
- Provisioning: `python scripts/provision_aws.py --env <staging|prod>`
- Dependencies pinned in `pyproject.toml`.

---

## Optional: Active ExecPlan Index

- `2026-02-23 - Initial Documentation and Scaffolding - status: done - owner: codex agent`

---

# AWS Provisioning Vertical Slice A

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Implement a real, idempotent AWS provisioning path that supports both `staging` and `prod` while remaining safe-by-default. Operators should be able to run plan mode first, then apply mode after filling `.env` with AWS credentials and networking settings.

## Progress

- [x] (2026-02-23 00:00Z) Initial planning completed.
- [x] Provisioning implementation milestone completed.
- [x] Local validation and documentation updates completed.
- [x] Live AWS apply validation completed for staging (resources created; RDS in `creating` state).

## Surprises & Discoveries

- Observation: Existing `.env` did not yet contain required AWS networking fields for RDS provisioning.
  Evidence: `AWS_VPC_ID` and `AWS_PRIVATE_SUBNET_IDS` were absent prior to `.env.template` update.
- Observation: Initial runtime role trust policy failed with `MalformedPolicyDocument` due principal reference timing/validation.
  Evidence: AWS returned `Invalid principal in policy` for `arn:aws:iam::<account>:role/tlg-macro-staging-gha-role`.

## Decision Log

- Decision: Make provisioning script default to plan-only mode and require explicit `--apply`.
  Rationale: Prevent accidental resource creation and support safer operator workflow.
  Date/Author: 2026-02-23, codex agent

- Decision: Require explicit network parameters for RDS creation (`AWS_VPC_ID`, `AWS_PRIVATE_SUBNET_IDS`).
  Rationale: Keeps networking assumptions explicit and avoids creating database resources in unintended networks.
  Date/Author: 2026-02-23, codex agent

## Outcomes & Retrospective

`scripts/provision_aws.py` now performs a real provisioning flow in apply mode (S3, IAM, CloudWatch, SNS, RDS/security-group/subnet-group) with idempotent checks. A plan-only mode prints resource names/outputs safely without mutations. Unit tests cover planning and normalization helpers. Staging apply has been executed successfully; supporting resources exist and RDS is provisioning.

## Context and Orientation

Key files touched in this slice:
- `scripts/provision_aws.py` for plan/apply logic and AWS API calls,
- `src/macro_data_ingest/config.py` for additional provisioning environment fields,
- `.env.template` for required settings,
- `docs/setup.md` and `README.md` for operator instructions,
- `tests/test_provision_plan.py` for provisioning-plan unit coverage.

## Plan of Work

1. Replace provisioning scaffold with plan/apply implementation and idempotent ensures.
2. Extend config surface with required AWS/network/database settings.
3. Add tests for deterministic plan behavior.
4. Update setup documentation for plan-before-apply workflow.
5. Validate with lint and unit tests only (no live apply yet).

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    .venv/bin/python scripts/provision_aws.py --env staging

Expected outcomes:
- tests/lint pass;
- plan mode prints summary and no resources are created.

## Validation and Acceptance

Slice acceptance criteria:
- Provisioning CLI supports `--env` and explicit `--apply`.
- Plan mode performs no mutations and emits copyable outputs.
- Apply mode path includes idempotent checks for required resources.
- Docs describe required environment variables and sequence.
- Unit tests pass for planning helpers.

## Idempotence and Recovery

Idempotence:
- Existing S3 buckets, IAM roles, log groups, subnet groups, SG rules, and RDS instance are reused.
- Existing SNS ARN from config is reused when supplied.

Recovery:
- Re-run `--apply` after partial failure; ensures will reuse already-created resources.
- For rollback, delete resources in reverse dependency order (RDS -> SG/SubnetGroup -> IAM/SNS/Logs -> S3).

## Artifacts and Notes

Artifacts:
- `scripts/provision_aws.py`
- `tests/test_provision_plan.py`
- `docs/setup.md`
- `.env.template`

Evidence snapshot:
- apply actions reported created/reused staging resources;
- follow-up check with `.env` credentials shows `tlg-macro-staging-pg` status `creating`.

## Interfaces and Dependencies

Interfaces:
- `python scripts/provision_aws.py --env <staging|prod>`
- `python scripts/provision_aws.py --env <staging|prod> --apply`

Dependencies:
- `boto3` and `botocore` for AWS API operations.

---

## Optional: Active ExecPlan Index

- `2026-02-23 - Initial Documentation and Scaffolding - status: done - owner: codex agent`
- `2026-02-23 - AWS Provisioning Vertical Slice A - status: done (staging applied) - owner: codex agent`

---

# BEA Ingest to Bronze Vertical Slice B

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Implement a real ingestion path from BEA API to S3 Bronze with run manifests and hash-based change detection so daily jobs can skip unchanged payload rewrites while preserving run-level lineage.

## Progress

- [x] (2026-03-03 00:00Z) Initial planning completed.
- [x] Ingestion implementation milestone completed.
- [x] Validation and documentation updates completed.

## Surprises & Discoveries

- Observation: default table value `SQPCE` did not work with Regional dataset in this environment.
  Evidence: BEA API returned `Invalid Value for Parameter TableName`.
- Observation: smoke mode using current year returned zero rows due data availability lag.
  Evidence: first smoke run returned `rows=0`; fallback-year strategy returned `rows=60`.

## Decision Log

- Decision: Use `SAPCE3` as default BEA table name for state-level PCE by type.
  Rationale: Valid Regional table and aligned to project scope.
  Date/Author: 2026-03-03, codex agent
- Decision: Always write run manifest and checkpoint, but write raw payload only when hash changes.
  Rationale: Preserves lineage while avoiding unnecessary raw rewrites.
  Date/Author: 2026-03-03, codex agent

## Outcomes & Retrospective

`mdi ingest` now performs a full BEA->Bronze flow with idempotent change detection. A real smoke run succeeded against staging and a second rerun showed no-op raw write behavior (`changed=False`) while still recording a manifest.

## Context and Orientation

Key files for this slice:
- `src/macro_data_ingest/ingest/bea_client.py` for API request/response handling,
- `src/macro_data_ingest/ingest/bronze_writer.py` for S3 payload/manifest/checkpoint operations,
- `src/macro_data_ingest/ingest/pipeline.py` for ingest orchestration and hash checkpoint logic,
- `src/macro_data_ingest/cli.py` for CLI wiring and run output,
- `tests/test_bea_client.py` and `tests/test_ingest_pipeline.py` for unit coverage.

## Plan of Work

1. Implement BEA client request/validation behavior.
2. Implement S3 Bronze writer with deterministic key layout.
3. Implement ingest orchestration with hash checkpoint comparison.
4. Wire `mdi ingest` to execute orchestrator.
5. Add/expand tests and run smoke validation against staging.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    BEA_TABLE_NAME=SAPCE3 .venv/bin/mdi ingest --env staging --smoke
    BEA_TABLE_NAME=SAPCE3 .venv/bin/mdi ingest --env staging --smoke

Expected outcomes:
- lint/tests pass;
- first ingest writes raw payload (`changed=True`);
- second ingest skips raw payload rewrite (`changed=False`) and writes manifest.

## Validation and Acceptance

Acceptance checks achieved:
- BEA API call is implemented and validates error payloads.
- Bronze payloads are stored under deterministic partitioned keys.
- Run manifests are written per run with request params/hash/row counts.
- Checkpoint hash is persisted and used for change detection on reruns.
- Unit tests pass and staging smoke run demonstrates idempotent behavior.

## Idempotence and Recovery

Idempotence:
- On matching payload hash, raw payload write is skipped.
- Manifest/checkpoint are still updated for run traceability.

Recovery:
- Re-run `mdi ingest` with same parameters after transient failures; unchanged payload path no-ops raw writes.

## Artifacts and Notes

Artifacts:
- `src/macro_data_ingest/ingest/bea_client.py`
- `src/macro_data_ingest/ingest/bronze_writer.py`
- `src/macro_data_ingest/ingest/pipeline.py`
- `src/macro_data_ingest/cli.py`
- `tests/test_bea_client.py`
- `tests/test_ingest_pipeline.py`

Evidence snapshot:
- `make lint test PYTHON=.venv/bin/python` -> pass (`10 passed`);
- smoke ingest run wrote Bronze payload and manifest;
- immediate rerun reported `changed=False` with skipped raw write.

## Interfaces and Dependencies

Interfaces:
- `mdi ingest --env <staging|prod> [--smoke] [--run-id <id>]`

Dependencies:
- `requests` for BEA API calls.
- `boto3` for S3 checkpoint/payload/manifest writes.

---

## Optional: Active ExecPlan Index

- `2026-02-23 - Initial Documentation and Scaffolding - status: done - owner: codex agent`
- `2026-02-23 - AWS Provisioning Vertical Slice A - status: done (staging applied) - owner: codex agent`
- `2026-03-03 - BEA Ingest to Bronze Vertical Slice B - status: done - owner: codex agent`

---

# Silver Normalization and Quality Checks Vertical Slice C

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Implement a real transform stage that reads latest Bronze BEA payloads, normalizes them into a stable Silver schema, enforces baseline data quality checks, and writes Parquet + manifest outputs to S3.

## Progress

- [x] (2026-03-03 00:00Z) Initial planning completed.
- [x] Transformation implementation milestone completed.
- [x] Validation and documentation updates completed.

## Surprises & Discoveries

- Observation: The newest Bronze run may not contain a payload object when ingest detects no change.
  Evidence: no-change ingest run wrote manifest/checkpoint only.
- Observation: Bronze payload includes nation and BEA region records in addition to states.
  Evidence: sample payload included `GeoFips` values `00000` and `91xxx`-`98xxx`.

## Decision Log

- Decision: Transform stage finds the latest available Bronze payload object instead of relying on latest run folder.
  Rationale: Supports ingest no-change runs while still enabling transforms.
  Date/Author: 2026-03-03, codex agent
- Decision: Silver output for this slice keeps only state + DC rows.
  Rationale: Aligns with immediate state-level serving scope and stabilizes keys.
  Date/Author: 2026-03-03, codex agent

## Outcomes & Retrospective

`mdi transform` now reads Bronze payloads from S3, creates typed state-level Silver records, validates non-null/uniqueness checks, and writes Parquet + manifest to Silver S3 paths. Smoke validation succeeded against staging with 51 records.

## Context and Orientation

Key files for this slice:
- `src/macro_data_ingest/transforms/silver.py` for normalization and DQ checks,
- `src/macro_data_ingest/transforms/pipeline.py` for S3 IO and transform orchestration,
- `src/macro_data_ingest/cli.py` for transform command execution,
- `tests/test_silver_transform.py` for transform and quality-check unit coverage,
- `docs/architecture.md` for updated partitioning examples.

## Plan of Work

1. Implement Silver transform schema normalization.
2. Add baseline DQ checks (row count, non-null required keys, uniqueness).
3. Implement transform pipeline read/write over S3 Bronze/Silver layers.
4. Wire CLI command and run unit tests.
5. Run staging smoke transform and inspect resulting Parquet sample.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    .venv/bin/mdi transform --env staging --smoke

Expected outcomes:
- lint/tests pass;
- transform writes Silver parquet + manifest with valid row counts.

## Validation and Acceptance

Acceptance checks achieved:
- Silver transform produces stable typed columns.
- DQ checks fail on empty/null/duplicate-key violations.
- Transform command writes Silver outputs under deterministic keys.
- Unit tests pass and staging smoke run completes successfully.

## Idempotence and Recovery

Idempotence:
- Re-running transform creates deterministic partitioned run outputs.
- Input discovery remains stable by selecting latest Bronze payload object.

Recovery:
- If transform fails after write, rerun with a new `run_id`; previous output is immutable and traceable via manifest.

## Artifacts and Notes

Artifacts:
- `src/macro_data_ingest/transforms/silver.py`
- `src/macro_data_ingest/transforms/pipeline.py`
- `src/macro_data_ingest/cli.py`
- `tests/test_silver_transform.py`
- `docs/architecture.md`

Evidence snapshot:
- `make lint test PYTHON=.venv/bin/python` -> pass (`13 passed`);
- staging smoke transform wrote Silver parquet with `rows=51`.

## Interfaces and Dependencies

Interfaces:
- `mdi transform --env <staging|prod> [--smoke] [--run-id <id>]`

Dependencies:
- `boto3` for S3 reads/writes.
- `pandas` + `pyarrow` for Silver Parquet output.

---

## Optional: Active ExecPlan Index

- `2026-02-23 - Initial Documentation and Scaffolding - status: done - owner: codex agent`
- `2026-02-23 - AWS Provisioning Vertical Slice A - status: done (staging applied) - owner: codex agent`
- `2026-03-03 - BEA Ingest to Bronze Vertical Slice B - status: done - owner: codex agent`
- `2026-03-03 - Silver Normalization and Quality Checks Vertical Slice C - status: done - owner: codex agent`

---

# Gold Modeling and Postgres Load Vertical Slice D

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Implement Gold modeling and Postgres serving load so Silver data can be upserted into a curated table and queried through a derived YoY view.

## Progress

- [x] (2026-03-03 00:00Z) Initial planning completed.
- [x] Gold modeling and loader implementation milestone completed.
- [x] Unit validation completed.
- [x] Live staging load validation completed.

## Surprises & Discoveries

- Observation: live `mdi load --env staging --smoke` timed out connecting to RDS from this machine.
  Evidence: `psycopg.errors.ConnectionTimeout` against `tlg-macro-staging-pg...rds.amazonaws.com`.

## Decision Log

- Decision: keep RDS private and document network path requirement instead of weakening default security posture.
  Rationale: aligns with least-privilege/network isolation; local validation can proceed once VPN/bastion path is available.
  Date/Author: 2026-03-03, codex agent
- Decision: establish baseline serving objects now (`gold.pce_state_annual`, `serving.v_pce_state_yoy`, `meta.ingest_runs`).
  Rationale: provides minimal but complete serving contract for downstream BI/notebook usage.
  Date/Author: 2026-03-03, codex agent

## Outcomes & Retrospective

Load stage now reads latest Silver parquet, models Gold records, upserts idempotently into Postgres, maintains run metadata, and refreshes a YoY serving view. Unit tests pass and staging live load has been validated end-to-end.

## Context and Orientation

Key files for this slice:
- `src/macro_data_ingest/transforms/gold.py` for Gold frame modeling,
- `src/macro_data_ingest/load/postgres_loader.py` for schema/table/view management and upserts,
- `src/macro_data_ingest/load/pipeline.py` for load orchestration from S3 to Postgres,
- `src/macro_data_ingest/cli.py` for load command wiring,
- `tests/test_gold_transform.py` and `tests/test_postgres_loader.py` for unit coverage.

## Plan of Work

1. Implement Gold transformation from Silver frame.
2. Implement Postgres loader DDL, upsert, serving view refresh, and run metadata recording.
3. Implement load pipeline to discover latest Silver parquet and execute load.
4. Wire `mdi load` command.
5. Validate with lint/tests and attempt staging smoke load.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    .venv/bin/mdi load --env staging --smoke

Expected outcomes:
- tests/lint pass;
- load command succeeds when DB network path is available.

## Validation and Acceptance

Acceptance achieved:
- Gold frame modeling implemented with stable columns and scaled values.
- Idempotent upsert implemented using Postgres `ON CONFLICT`.
- Serving view `serving.v_pce_state_yoy` created/refreshed.
- Run metadata persisted to `meta.ingest_runs`.
- Unit tests pass.
- Staging live load succeeds with `rows=51`.
- Idempotent rerun behavior confirmed (gold row count remains stable across reruns).

## Idempotence and Recovery

Idempotence:
- Re-runs of load update existing primary keys deterministically.
- View refresh is `CREATE OR REPLACE`.
- Run metadata is upserted by `run_id`.

Recovery:
- If partial DB changes occur, re-run safely with new `run_id` due upsert semantics.

## Artifacts and Notes

Artifacts:
- `src/macro_data_ingest/transforms/gold.py`
- `src/macro_data_ingest/load/postgres_loader.py`
- `src/macro_data_ingest/load/pipeline.py`
- `src/macro_data_ingest/cli.py`
- `tests/test_gold_transform.py`
- `tests/test_postgres_loader.py`

Evidence snapshot:
- `make lint test PYTHON=.venv/bin/python` -> pass (`16 passed`);
- live staging load succeeded and wrote `51` rows to `gold.pce_state_annual`;
- second live load rerun succeeded with `gold.pce_state_annual` row count still `51` (no duplication).

## Interfaces and Dependencies

Interfaces:
- `mdi load --env <staging|prod> [--smoke] [--run-id <id>]`

Dependencies:
- `sqlalchemy` + `psycopg` for Postgres access.
- `boto3` for Silver source and Gold manifest S3 operations.

---

## Optional: Active ExecPlan Index

- `2026-02-23 - Initial Documentation and Scaffolding - status: done - owner: codex agent`
- `2026-02-23 - AWS Provisioning Vertical Slice A - status: done (staging applied) - owner: codex agent`
- `2026-03-03 - BEA Ingest to Bronze Vertical Slice B - status: done - owner: codex agent`
- `2026-03-03 - Silver Normalization and Quality Checks Vertical Slice C - status: done - owner: codex agent`
- `2026-03-03 - Gold Modeling and Postgres Load Vertical Slice D - status: done - owner: codex agent`

---

# CI and Scheduler Hardening Vertical Slice E

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Harden CI/scheduler behavior so automated runs are traceable, stable, and operationally debuggable with explicit run metadata and artifact capture.

## Progress

- [x] (2026-03-03 00:00Z) Initial planning completed.
- [x] Workflow and CLI hardening implementation completed.
- [x] Validation and retrospective updates completed.

## Surprises & Discoveries

- Observation: `mdi run-all` generated separate run IDs per stage unless an explicit `--run-id` was provided.
  Evidence: stage handlers independently called `_resolve_run_id`.

## Decision Log

- Decision: force a shared run ID in `run-all` when caller does not provide one.
  Rationale: preserves stage lineage correlation and easier debugging.
  Date/Author: 2026-03-03, codex agent
- Decision: make ingest workflow emit local artifacts (`logs/pipeline.log`, `manifests/run_context.json`) every run.
  Rationale: improves failure triage directly in GitHub Actions UI.
  Date/Author: 2026-03-03, codex agent

## Outcomes & Retrospective

Workflow and CLI hardening were validated locally with `make lint test` and a full staging smoke `mdi run-all`. All stages executed successfully under a shared `run_id`, and outputs were written across Bronze/Silver/Gold with consistent lineage.

## Context and Orientation

Key files:
- `src/macro_data_ingest/cli.py` for shared run ID behavior in `run-all`,
- `.github/workflows/ingest.yml` for scheduler hardening,
- `README.md` and `docs/setup.md` for updated operational guidance.

## Plan of Work

1. Fix run ID consistency in `run-all`.
2. Add workflow permissions/concurrency/timeout safeguards.
3. Add per-run artifact generation and upload support.
4. Update docs to reflect implemented (not scaffold) state.
5. Validate with lint/tests and local smoke `mdi run-all`.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    .venv/bin/mdi run-all --env staging --smoke

Expected outcomes:
- lint/tests pass;
- local run-all succeeds with shared `run_id` through all stages.

## Validation and Acceptance

Acceptance checks:
- `run-all` uses one run ID across ingest/transform/load.
- scheduler workflow captures logs/manifests artifacts.
- docs reflect current implementation maturity.

## Idempotence and Recovery

Idempotence:
- Re-runs preserve deterministic behavior with explicit run IDs.

Recovery:
- Workflow artifacts provide the run context needed to retry failed runs.

## Artifacts and Notes

Artifacts:
- `src/macro_data_ingest/cli.py`
- `.github/workflows/ingest.yml`
- `README.md`
- `docs/setup.md`

## Interfaces and Dependencies

Interfaces:
- `mdi run-all --env <staging|prod> [--smoke] [--run-id <id>]`

Dependencies:
- GitHub Actions runtime with configured repository secrets.

---

# Vintage Strategy and SAPCE3 Historical Backfill

This ExecPlan is a living document and follows `.agent/PLANS.md`.

## Purpose / Big Picture

Implement a lightweight vintage-aware ingest strategy where daily runs request a configured full annual range (from a start year through current year), use payload hashes to gate downstream processing, and persist vintage metadata for reproducibility. Immediately use that strategy to backfill SAPCE3 from year 2000 onward.

## Progress

- [x] (2026-03-03 00:00Z) Initial planning completed.
- [x] Code updates for configurable start-year range and vintage metadata completed.
- [x] Change-gated `run-all` behavior implemented.
- [x] Validation and live SAPCE3 backfill run completed.

## Surprises & Discoveries

- Observation: Existing `run-all` always executed transform/load even when ingest payload hash was unchanged.
  Evidence: `src/macro_data_ingest/cli.py` called stage commands sequentially without a changed gate.
- Observation: Hashing full BEA payload caused false positives because `UTCProductionTime` changes every API response.
  Evidence: two immediate SAPCE3 runs both showed `changed=True` until hash scope was narrowed to normalized `Results.Data` rows.

## Decision Log

- Decision: Use full-range daily requests (`BEA_START_YEAR` to current year) with payload-hash change detection.
  Rationale: Keeps revision handling simple at current data volume while avoiding unnecessary downstream work on unchanged payloads.
  Date/Author: 2026-03-03, codex agent

## Outcomes & Retrospective

Implemented and validated end-to-end. Lint/tests pass (`22 passed`), SAPCE3 backfill ran successfully from year 2000 through current year, and an immediate rerun confirmed no-op downstream behavior when row content is unchanged. This preserves revision coverage while avoiding unnecessary transform/load cycles on stable source data.

## Context and Orientation

Key files:
- `src/macro_data_ingest/config.py` for env-driven ingest range settings.
- `src/macro_data_ingest/ingest/pipeline.py` and `src/macro_data_ingest/ingest/bronze_writer.py` for vintage/hash checkpoint metadata.
- `src/macro_data_ingest/cli.py` for change-gated orchestration.
- `docs/operability.md`, `docs/setup.md`, `.env.template` for runbook/config updates.

## Plan of Work

1. Add `BEA_START_YEAR` configuration and derive explicit request year ranges.
2. Extend checkpoint/manifest metadata with vintage context.
3. Gate transform/load when ingest payload hash is unchanged.
4. Add tests for year range and run-all skip behavior.
5. Run lint/tests and execute SAPCE3 backfill (2000-current).

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make lint test PYTHON=.venv/bin/python
    BEA_TABLE_NAME=SAPCE3 BEA_START_YEAR=2000 .venv/bin/mdi run-all --env staging --run-id backfill-sapce3-2000

Expected outcomes:
- tests and lint pass;
- ingest writes/reuses Bronze payload and checkpoint with vintage metadata;
- transform/load only run when ingest changes.

## Validation and Acceptance

Acceptance checks:
- Ingest query year range starts at configured `BEA_START_YEAR`.
- Checkpoint includes `requested_year_range` and payload hash lineage.
- `run-all` skips transform/load on unchanged payload.
- SAPCE3 historical backfill completes for 2000-current or reports explicit external blocker.

## Idempotence and Recovery

Idempotence:
- Re-running with same source payload hash no-ops downstream transforms/load.
- Postgres upsert remains key-based and deterministic.

Recovery:
- If run fails after ingest, rerun with a new run ID; unchanged payload gates duplicate downstream work.

## Artifacts and Notes

Primary artifacts:
- `src/macro_data_ingest/config.py`
- `src/macro_data_ingest/ingest/pipeline.py`
- `src/macro_data_ingest/ingest/bronze_writer.py`
- `src/macro_data_ingest/cli.py`
- `tests/test_ingest_pipeline.py`
- `tests/test_cli.py`
- `docs/setup.md`
- `docs/operability.md`
- `.env.template`

## Interfaces and Dependencies

Interfaces:
- Env var: `BEA_START_YEAR`
- CLI: `mdi run-all --env <staging|prod> --run-id <id>`

Dependencies:
- BEA API availability and credentials,
- AWS S3 and Postgres connectivity for live backfill.

---

# Function Name Propagation for Gold PCE State Annual

This ExecPlan is a living document and follows `.agent/PLANS.md`.

Links: branch `feature/pce-function-name`; feature brief `.agent/features/2026-03-04-pce-function-name/SPEC.md`; PR `https://github.com/john-e-moore/macro-data-ingest/pull/4`.

## Purpose / Big Picture

Expose a human-readable BEA function label directly in `gold.pce_state_annual` so analysts can query category names without external joins. The observable result is a new non-null `function_name` column stored between `series_code` and `pce_value` and populated during ingest/transform/load.

## Progress

- [x] (2026-03-04 00:00Z) Initial planning completed.
- [x] Implementation across ingest, transforms, and load completed.
- [x] Validation and documentation updates completed.

## Surprises & Discoveries

- Observation: `--smoke` runs for `LineCode=ALL` can return zero rows when the prior calendar year is not published for a table.
  Evidence: ingest error `BEA returned zero rows ... year=2025 ... line_code=ALL`.
- Observation: full live pulls for all line codes may hit BEA API rate limits (HTTP 429) in local validation.
  Evidence: ingest traceback with `429 Client Error: Too Many Requests` on `GetData`.

## Decision Log

- Decision: source `function_name` from BEA `GetParameterValuesFiltered` `ParamValue.Desc` metadata and map by `LineCode`.
  Rationale: BEA payload rows do not consistently include a durable function label field, but metadata endpoint provides canonical line descriptions.
  Date/Author: 2026-03-04, codex agent
- Decision: keep migration additive (`ADD COLUMN IF NOT EXISTS`) and backfill nulls to empty string before `NOT NULL`.
  Rationale: safe rollout for existing deployments with minimal operational risk.
  Date/Author: 2026-03-04, codex agent
- Decision: validate pipeline end-to-end using a minimal one-line-code dataset config for live run evidence.
  Rationale: avoids transient BEA 429 limits while still proving ingest/transform/load and Postgres schema changes.
  Date/Author: 2026-03-04, codex agent

## Outcomes & Retrospective

Implementation is complete across client, ingest, Silver, Gold, Postgres load DDL/migration, and tests. Validation succeeded with local lint/tests (`31 passed`) plus a live staging run (ingest + transform + load) using a minimal dataset config to avoid BEA 429 throttling while still exercising end-to-end behavior. Postgres verification confirms `function_name` is physically between `series_code` and `pce_value` and populated with BEA function labels.

## Context and Orientation

Relevant paths:
- `src/macro_data_ingest/ingest/bea_client.py` for BEA metadata lookup.
- `src/macro_data_ingest/ingest/pipeline.py` for row-level function-name enrichment during `LineCode=ALL` expansion.
- `src/macro_data_ingest/transforms/silver.py` and `src/macro_data_ingest/transforms/gold.py` for contract propagation.
- `src/macro_data_ingest/load/postgres_loader.py` for table schema and migration behavior.
- `tests/test_bea_client.py`, `tests/test_ingest_pipeline.py`, `tests/test_silver_transform.py`, `tests/test_gold_transform.py` for behavior coverage.

## Plan of Work

1. Add BEA line-code description mapping method in client.
2. Enrich ingested rows with `FunctionName` prior to Bronze write.
3. Extend Silver and Gold contracts with `function_name`.
4. Add Postgres schema migration + DDL ordering requirement.
5. Add/update unit tests.
6. Run lint/tests, execute end-to-end validation run, and verify DB column shape/data.
7. Prepare PR using `.agent/PR_TEMPLATE.md`.

## Concrete Steps

    cd /home/john/tlg/macro-data-ingest
    make install lint test PYTHON=.venv/bin/python
    .venv/bin/mdi run-all --env staging --run-id function-name-validation --dataset-id pce_state_sapce3

Expected outcomes:
- lint/tests pass locally;
- pipeline stages complete and load applies schema migration;
- Postgres shows `function_name` column and populated values.

## Validation and Acceptance

Acceptance checks:
- BEA client returns line-code description mapping.
- Ingest rows contain `FunctionName` when line metadata is available.
- Silver/Gold include `function_name` column.
- Postgres table includes `function_name` between `series_code` and `pce_value`.
- Idempotent upsert behavior remains unchanged.

## Idempotence and Recovery

Idempotence:
- Existing primary key and upsert logic are unchanged.
- Additive column migration is repeatable (`IF NOT EXISTS`, deterministic null backfill).

Recovery:
- If ingest hits BEA rate limits, rerun with reduced dataset scope for validation, then rerun full scheduled pipeline.
- If load fails mid-run, rerun load safely; migration and upsert are idempotent.

## Artifacts and Notes

Artifacts:
- `.agent/features/2026-03-04-pce-function-name/SPEC.md`
- `src/macro_data_ingest/ingest/bea_client.py`
- `src/macro_data_ingest/ingest/pipeline.py`
- `src/macro_data_ingest/transforms/silver.py`
- `src/macro_data_ingest/transforms/gold.py`
- `src/macro_data_ingest/load/postgres_loader.py`
- `tests/test_bea_client.py`
- `tests/test_ingest_pipeline.py`
- `tests/test_silver_transform.py`
- `tests/test_gold_transform.py`
- `docs/architecture.md`

Evidence snippets:
- `make lint test PYTHON=.venv/bin/python` -> `All checks passed!` and `31 passed`.
- `DATASETS_CONFIG_PATH=/tmp/datasets-function-name.yaml .venv/bin/mdi run-all --env staging --run-id function-name-validate-20260304 --dataset-id pce_state_sapce3_lc1` -> ingest/transform/load completed.
- Postgres verification query output:
  - `col_order [..., 'series_code', 'function_name', 'pce_value', ...]`
  - `function_between True`
  - `function_name_groups [('[SAPCE3] Total personal consumption expenditures: Personal consumption expenditures', 51)]`

## Interfaces and Dependencies

Interfaces:
- `BeaClient.fetch_line_code_descriptions(dataset, table_name) -> dict[str, str]`
- Gold table contract `gold.pce_state_annual(..., series_code, function_name, pce_value, ...)`

Dependencies:
- BEA API (`GetParameterValuesFiltered`, `GetData`)
- AWS S3 for Bronze/Silver artifacts
- Postgres for Gold table migration and upsert

---

## Optional: Active ExecPlan Index

- `2026-02-23 - Initial Documentation and Scaffolding - status: done - owner: codex agent`
- `2026-02-23 - AWS Provisioning Vertical Slice A - status: done (staging applied) - owner: codex agent`
- `2026-03-03 - BEA Ingest to Bronze Vertical Slice B - status: done - owner: codex agent`
- `2026-03-03 - Silver Normalization and Quality Checks Vertical Slice C - status: done - owner: codex agent`
- `2026-03-03 - Gold Modeling and Postgres Load Vertical Slice D - status: done - owner: codex agent`
- `2026-03-03 - CI and Scheduler Hardening Vertical Slice E - status: done - owner: codex agent`
- `2026-03-03 - Vintage Strategy and SAPCE3 Historical Backfill - status: done - owner: codex agent`
- `2026-03-04 - Function Name Propagation for Gold PCE State Annual - status: done - owner: codex agent`
- `2026-03-04 - Standardized Backfill SOP and Function Name Metadata Repair - status: done - owner: codex agent`
