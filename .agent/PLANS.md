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
