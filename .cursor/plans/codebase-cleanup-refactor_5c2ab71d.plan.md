---
name: codebase-cleanup-refactor
overview: Implement a focused cleanup pass to remove dead compatibility surfaces, consolidate duplicated pipeline/client code, and tighten documentation so it matches the simplified serving model.
todos:
  - id: phase1-quick-wins
    content: Remove dead/unused surfaces and no-op code paths with minimal behavior change.
    status: pending
  - id: phase2-remove-legacy-fallback
    content: Drop dataset config legacy fallback and update tests/docs to enforce explicit datasets config.
    status: pending
  - id: phase3-shared-utils
    content: Extract shared HTTP retry and S3 stage I/O helpers; refactor clients/pipelines to use them.
    status: pending
  - id: phase4-cli-dedupe
    content: Refactor CLI stage orchestration into shared helpers while preserving command behavior.
    status: pending
  - id: phase5-loader-modularize
    content: Split Postgres loader responsibilities into smaller units and maintain SQL contract parity.
    status: pending
  - id: phase6-docs-consolidation
    content: Consolidate dataset documentation, trim README command sprawl, and fix roadmap/workflow wording.
    status: pending
  - id: phase-validation
    content: Run lint/tests/smoke after each phase and address regressions before proceeding.
    status: pending
isProject: false
---

# Codebase Cleanup Implementation Plan

## Goals

- Remove now-unnecessary compatibility and dead code paths.
- Consolidate repeated logic in CLI, API clients, and S3 stage helpers.
- Reduce maintenance overhead in docs by centralizing dataset/runtime guidance.
- Keep behavior stable via incremental refactors with test coverage at each step.

## Phase 1: Low-Risk Quick Wins

- Remove unused/dead surfaces first, with minimal behavior change.
- Update tests that assert legacy behavior only where intended deprecation is accepted.

Primary targets:

- `[src/macro_data_ingest/cli.py](src/macro_data_ingest/cli.py)`: remove unused `--env` argument plumbing if `APP_ENV` remains source of truth.
- `[src/macro_data_ingest/config.py](src/macro_data_ingest/config.py)`: remove or wire currently unused `app_env` field.
- `[src/macro_data_ingest/run_metadata.py](src/macro_data_ingest/run_metadata.py)`: delete unused `stable_payload_hash()` if no call sites.
- `[src/macro_data_ingest/transforms/silver.py](src/macro_data_ingest/transforms/silver.py)`: simplify `_parse_state_fips()` redundant branch.
- `[src/macro_data_ingest/ingest/pipeline.py](src/macro_data_ingest/ingest/pipeline.py)`: remove duplicate `source_release_tag` assignment and any no-op variables.

## Phase 2: Remove Legacy Compatibility Fallback

- Decide and enforce canonical config source (`config/datasets.yaml`).
- Remove fallback behavior that auto-generates a dataset when config file is missing.

Primary targets:

- `[src/macro_data_ingest/datasets.py](src/macro_data_ingest/datasets.py)`: remove `_legacy_default_spec()` path and require explicit datasets config.
- `[tests/test_datasets.py](tests/test_datasets.py)`: replace legacy fallback test with explicit validation/error test for missing config.
- `[README.md](README.md)`, `[docs/setup.md](docs/setup.md)`: update setup text to reflect required datasets config contract.

## Phase 3: Consolidate Repeated Infra Logic

- Extract shared HTTP request/retry/throttle behavior into one utility and reuse for BEA/Census clients.
- Extract shared S3 read/write/list-latest helper functions used by transform/load stages.

Primary targets:

- Add `[src/macro_data_ingest/ingest/http_client_base.py](src/macro_data_ingest/ingest/http_client_base.py)` (or equivalent utility module).
- Refactor `[src/macro_data_ingest/ingest/bea_client.py](src/macro_data_ingest/ingest/bea_client.py)` and `[src/macro_data_ingest/ingest/census_client.py](src/macro_data_ingest/ingest/census_client.py)` to reuse shared logic.
- Add `[src/macro_data_ingest/s3_io.py](src/macro_data_ingest/s3_io.py)` (or `utils/s3.py`) for JSON/parquet read/write and latest-key lookup.
- Refactor `[src/macro_data_ingest/transforms/pipeline.py](src/macro_data_ingest/transforms/pipeline.py)` and `[src/macro_data_ingest/load/pipeline.py](src/macro_data_ingest/load/pipeline.py)` to use shared S3 helpers.

## Phase 4: Reduce CLI Stage Duplication

- Introduce a generic dataset-stage execution helper used by `ingest`, `transform`, `load`, and `run-all`.
- Keep user-facing command behavior and output stable while reducing repeated loops/error handling.

Primary targets:

- `[src/macro_data_ingest/cli.py](src/macro_data_ingest/cli.py)`: centralize dataset iteration, per-dataset run-id creation, and exception handling.
- `[tests/test_cli.py](tests/test_cli.py)`: extend tests to cover unchanged `run-all` gating and command-level behavior parity.

## Phase 5: Tame Postgres Loader Complexity

- Split large responsibilities while preserving SQL contracts.
- Keep DDL/view SQL behavior unchanged initially; move structure first, then optimize.

Primary targets:

- `[src/macro_data_ingest/load/postgres_loader.py](src/macro_data_ingest/load/postgres_loader.py)`: split into focused units:
  - schema/bootstrap management,
  - dimension/fact upsert logic,
  - hierarchy sync logic,
  - serving-view definitions.
- Optional extraction of large view SQL into dedicated SQL files under a new path like `[sql/serving/](sql/serving/)`.
- Maintain/expand loader tests in `[tests/test_postgres_loader.py](tests/test_postgres_loader.py)` with additional unit tests around extracted helpers.

## Phase 6: Documentation Consolidation and Consistency

- Convert repeated dataset details into one canonical location and link from other docs.
- Shorten README command sprawl and align naming with current multi-source ingest reality.

Primary targets:

- `[README.md](README.md)`: replace long per-dataset command list with generic command patterns + config-driven explanation.
- Add `[docs/datasets.md](docs/datasets.md)`: canonical dataset catalog and semantics.
- Update `[docs/setup.md](docs/setup.md)`, `[docs/architecture.md](docs/architecture.md)`, `[docs/spec.md](docs/spec.md)` to link to dataset catalog instead of duplicating.
- Update `[docs/roadmap.md](docs/roadmap.md)`: remove already-delivered Census item from future scope.
- Rename workflow display title in `[.github/workflows/ingest.yml](.github/workflows/ingest.yml)` from BEA-only wording to source-agnostic wording.

## Validation and Rollout Strategy

- After each phase: run lint/tests and fix regressions before continuing.
- Keep refactors in small commits/PR slices so failures are easy to isolate.
- Focus on behavior-preserving refactors first; avoid semantic SQL changes until structure is stabilized.

Suggested validation checkpoints:

- `make lint`
- `make test`
- Smoke command on staging config: `mdi run-all --env staging --run-id <id> --smoke`

## Execution Order

1. Phase 1 quick wins.
2. Phase 2 legacy fallback removal.
3. Phase 3 shared utility consolidation.
4. Phase 4 CLI deduplication.
5. Phase 5 loader modularization.
6. Phase 6 docs cleanup/alignment.

This sequence minimizes risk by removing dead code first, then consolidating repeated infrastructure logic, and finally performing higher-touch structural and documentation refactors.