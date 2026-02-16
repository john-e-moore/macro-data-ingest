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
