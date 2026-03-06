# Dataset Catalog

The canonical dataset definitions live in `config/datasets.yaml`.

This document summarizes the current configured coverage and key semantics.

## BEA Datasets

Enabled annual tables:

- `pce_state_sapce1` (`SAPCE1`, `line_code: ALL`)
- `pce_state_sapce4` (`SAPCE4`, `line_code: ALL`)
- `state_regional_price_parities_sarpp` (`SARPP`, `line_code: ALL`)
- `state_real_income_and_pce_sarpi` (`SARPI`, `line_code: ALL`)
- `state_gdp_sagdp1` through `state_gdp_sagdp9` (`line_code: ALL`)
- `state_gdp_sagdp11` (`line_code: ALL`)
- `state_gdp_sasummary` (`line_code: ALL`)
- `state_personal_transfer_receipts_sainc35` (`SAINC35`, `line_code: 2000`)

Staged monthly entry (disabled by default):

- `pce_state_sapce4_monthly` (`SAPCE4`, `bea_frequency: M`)

Default start-year baseline for BEA datasets is `2000` unless explicitly narrowed.

## Census Datasets

Enabled annual series:

- `census_state_population`
  - `dataset_path: acs/acs1`
  - `variable: B01003_001E`
  - `start_year: 2000`
  - Includes pre-2005 intercensal backfill.

- `census_state_gov_finance_federal_intergovernmental_revenue`
  - `dataset_path: timeseries/govs`
  - `variable: AMOUNT`
  - `start_year: 2012`
  - Predicates: `SVY_COMP=02`, `GOVTYPE=002`, `AGG_DESC=SF0004`
  - Unit semantics: values are reported in thousands of dollars.

## Configuration Contract

- `config/datasets.yaml` is required at runtime.
- `enabled: true` controls which datasets run in `mdi run-all`.
- `--dataset-id` on CLI narrows execution to a single configured dataset.
