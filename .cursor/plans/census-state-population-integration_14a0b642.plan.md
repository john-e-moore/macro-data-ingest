---
name: census-state-population-integration
overview: Integrate a first Census datasource (state annual population) into the existing BEA-first pipeline with minimal disruption, then make it runnable in `mdi run-all` via datasets config.
todos:
  - id: spec-union-and-routing
    content: Generalize dataset spec model and CLI stage typing/routing for bea+census sources.
    status: pending
  - id: census-ingest
    content: Implement Census client and ingest pipeline branch with Bronze checkpoint/hash/manifest support.
    status: pending
  - id: census-transform-load
    content: Implement Census silver/gold/load path and Postgres population table upsert.
    status: pending
  - id: config-and-docs
    content: Add env/config entries for Census dataset and document run commands.
    status: pending
  - id: tests
    content: Add/extend tests for dataset parsing, ingest, transform, and load for Census source.
    status: pending
isProject: false
---

# Census State Population Integration Plan

## Scope and first dataset

- Add one Census dataset first: annual state population from Population Estimates (`pep/population`, variable `POP`).
- Keep geography and period aligned to current BEA joins: `state_fips` + annual `year` (and `period_code=YYYY`).
- Keep first cut simple: ingest/load this as a dedicated population table, then optionally add a per-capita serving view in the same pass.

## 1) Introduce source-discriminated dataset specs

- Refactor dataset config parsing in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/datasets.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/datasets.py)`:
  - Replace BEA-only `BeaDatasetSpec` with a source-discriminated model (e.g., base fields + `BeaDatasetSpec` and `CensusDatasetSpec`).
  - Keep shared fields (`dataset_id`, `source`, `storage_dataset`, `target_table`, `enabled`) and add Census-specific fields for this first table (e.g., `census_dataset_path`, `census_variable`, `census_geo`, `start_year`).
  - Preserve legacy BEA fallback behavior when datasets YAML is missing.
- Update CLI typing/imports and dataset resolution in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/cli.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/cli.py)` to accept the new union spec type.

## 2) Add Census ingest path (Bronze)

- Add a Census client module (new file, parallel to BEA client) under `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/ingest/](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/ingest/)` to call Census API and normalize raw rows for hashing.
- Extend ingest routing in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/ingest/pipeline.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/ingest/pipeline.py)`:
  - Dispatch on `dataset_spec.source` (`bea` existing behavior, `census` new behavior).
  - Reuse existing `BronzeWriter` checkpoint/hash/manifest pattern so change detection works consistently.
  - Add Census release/vintage metadata in manifest (at minimum requested year range and API path).

## 3) Add Census transform path (Silver)

- Add Census silver transformer (new file) under `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/transforms/](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/transforms/)` that maps Census payload to a typed state-annual frame.
- Update stage router in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/transforms/pipeline.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/transforms/pipeline.py)`:
  - Route BEA to existing `to_silver_frame`.
  - Route Census to new transformer and validator.
- Silver schema for Census first table should minimally include: `state_fips`, `state_abbrev`, `geo_name`, `frequency='A'`, `period_code`, `year`, `population`, plus source/table metadata needed downstream.

## 4) Add Census load path (Gold/Postgres)

- Add source-specific load routing in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/pipeline.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/pipeline.py)`:
  - Keep existing BEA load unchanged.
  - For Census, bypass BEA-specific `to_gold_frame` and use a Census gold projection.
- Extend DDL and upsert support in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/postgres_loader.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/load/postgres_loader.py)`:
  - Add `gold.population_state_annual` (or configured target table) keyed by `(state_fips, year)`.
  - Upsert population rows idempotently.
- Optional-but-useful in same change: add a serving view joining BEA annual OBT to population for per-capita outputs.

## 5) Config and env wiring

- Add Census API key support in `[/home/john/tlg/macro-data-ingest/src/macro_data_ingest/config.py](/home/john/tlg/macro-data-ingest/src/macro_data_ingest/config.py)` and template docs in `[/home/john/tlg/macro-data-ingest/.env.template](/home/john/tlg/macro-data-ingest/.env.template)`.
- Add first Census dataset entry to `[/home/john/tlg/macro-data-ingest/config/datasets.yaml](/home/john/tlg/macro-data-ingest/config/datasets.yaml)` alongside existing BEA entries.
- Mirror an example Census entry in `[/home/john/tlg/macro-data-ingest/config/datasets.example.yaml](/home/john/tlg/macro-data-ingest/config/datasets.example.yaml)`.

## 6) Tests and docs

- Extend dataset parsing/validation tests in `[/home/john/tlg/macro-data-ingest/tests/test_datasets.py](/home/john/tlg/macro-data-ingest/tests/test_datasets.py)`.
- Add ingest tests for Census query/build/hash/checkpoint behavior near `[/home/john/tlg/macro-data-ingest/tests/test_ingest_pipeline.py](/home/john/tlg/macro-data-ingest/tests/test_ingest_pipeline.py)`.
- Add transform/load tests for Census silver/gold frames and idempotent keys (new tests adjacent to existing transform/load tests).
- Update operator docs in `[/home/john/tlg/macro-data-ingest/README.md](/home/john/tlg/macro-data-ingest/README.md)` and `[/home/john/tlg/macro-data-ingest/docs/operability.md](/home/john/tlg/macro-data-ingest/docs/operability.md)` to include the new dataset and command examples.

## Implementation notes to minimize risk

- Keep BEA paths behaviorally identical; introduce source routing instead of deep rewrites.
- Use additive schema changes only (new table/view), no breaking rename in existing BEA tables/views.
- Maintain existing run-all semantics: if Census payload hash unchanged, skip transform/load for that dataset.

