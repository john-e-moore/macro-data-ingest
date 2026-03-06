---
name: federal-receipts-ratios
overview: Integrate Census ASFIN federal-intergovernmental receipts and BEA federal social benefits to persons, then publish state-level annual receipt-to-GDP ratios in serving views without adding new API keys.
todos:
  - id: feature-brief-execplan
    content: Create feature brief and ExecPlan entry per .agent workflow
    status: completed
  - id: asfin-source-integration
    content: Add Census ASFIN dataset spec and ingest/transform/load support
    status: completed
  - id: bea-federal-transfer-spec
    content: Add/confirm BEA federal-social-benefits dataset config for federal-only persons receipts
    status: completed
  - id: serving-ratio-views
    content: Implement two new annual serving ratio views against nominal state GDP
    status: completed
  - id: tests-and-docs
    content: Add tests, run validations, and update README/docs/spec/architecture
    status: completed
isProject: false
---

# Federal Receipts + GDP Ratios Integration Plan

## Goal

Add two clean federal-receipt channels to the existing mixed-source pipeline and expose annual state ratios versus nominal GDP:

- Federal-to-state-government receipts intensity (`ASFIN / GDP`)
- Federal-to-persons receipts intensity (`BEA federal social benefits / GDP`)

## Scope Decisions (locked)

- BEA measure: **federal social benefits to persons only**.
- Deliverable: **source integration + serving ratio views** in the same feature.

## Key Files To Update

- Dataset and source wiring:
  - [/home/john/tlg/macro-data-ingest/config/datasets.yaml](/home/john/tlg/macro-data-ingest/config/datasets.yaml)
  - [/home/john/tlg/macro-data-ingest/config/datasets.example.yaml](/home/john/tlg/macro-data-ingest/config/datasets.example.yaml)
  - [/home/john/tlg/macro-data-ingest/src/macro_data_ingest/datasets.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/datasets.py)
- Ingest/transform/load routing and contracts:
  - [/home/john/tlg/macro-data-ingest/src/macro_data_ingest/ingest/pipeline.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/ingest/pipeline.py)
  - [/home/john/tlg/macro-data-ingest/src/macro_data_ingest/transforms/pipeline.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/transforms/pipeline.py)
  - [/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/pipeline.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/pipeline.py)
  - [/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/postgres_loader.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/postgres_loader.py)
- Docs and tests:
  - [/home/john/tlg/macro-data-ingest/docs/spec.md](/home/john/tlg/macro-data-ingest/docs/spec.md)
  - [/home/john/tlg/macro-data-ingest/docs/architecture.md](/home/john/tlg/macro-data-ingest/docs/architecture.md)
  - [/home/john/tlg/macro-data-ingest/docs/setup.md](/home/john/tlg/macro-data-ingest/docs/setup.md)
  - [/home/john/tlg/macro-data-ingest/README.md](/home/john/tlg/macro-data-ingest/README.md)
  - [/home/john/tlg/macro-data-ingest/tests/test_datasets.py](/home/john/tlg/macro-data-ingest/tests/test_datasets.py)
  - add/extend source-specific ingest/transform/load tests near existing pipeline tests.

## Implementation Steps

1. Add a feature brief under `.agent/features/<date>-federal-receipts-ratios/` and add an ExecPlan entry in `.agent/PLANS.md` per `.agent/AGENTS.md`.
2. Introduce a Census ASFIN dataset spec (state-government federal intergovernmental revenue) in dataset config and parser validation.
3. Add/extend Census ingest path for ASFIN endpoint normalization using existing Census auth/config surface (no new keys).
4. Add a Census silver->gold projection for ASFIN annual state rows and idempotent Postgres upsert target.
5. Add/confirm BEA dataset config for federal social benefits to persons by state (federal-only transfer concept), mapped into existing BEA ingest path.
6. Extend serving view generation to publish two annual ratios keyed by `state_fips, year` using nominal GDP denominator from existing BEA GDP series.
7. Update load manifest `target_views` reporting to include the new ratio views.
8. Add tests for dataset parsing, routing, transform contracts, ratio SQL logic, and idempotent reruns.
9. Update operator docs (definitions, interpretation caveats, and exact dataset IDs/view names).

## Proposed Serving Outputs

- `serving.v_state_federal_to_stategov_gdp_annual`
- `serving.v_state_federal_to_persons_gdp_annual`
- Optional helper view (if useful): `serving.v_state_federal_receipts_channels_annual` (wide form with both numerators + GDP)

## Validation Plan

- Run unit tests for datasets, ingest, transforms, and load/view generation.
- Run smoke `mdi run-all` for each new dataset ID in staging.
- Verify view row counts and null behavior for denominator gaps.
- Verify idempotence: immediate rerun yields stable row counts and no duplicate facts.
- Run lint checks and address introduced diagnostics.

## Non-Goals

- No attempt to create a single “total federal received” dollar sum across incompatible accounting systems.
- No IRS/BLS integration in this feature.
- No new credentials/secrets beyond existing BEA and Census keys.

