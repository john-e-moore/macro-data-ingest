# Federal Receipts Ratios

## Scope

Integrate two annual state-level federal-receipt channels and publish GDP-normalized serving views:

1. BEA `SAINC35` line `2000` (transfer receipts of individuals from governments) as the
   persons channel.
2. Census `timeseries/govs` state government finance (`SVY_COMP=02`, `GOVTYPE=002`,
   `AGG_DESC=SF0004`) as the state-government channel.

Ratios are computed against nominal state GDP using BEA `SAGDP1` line `3`.

## Acceptance Criteria

- Both new dataset IDs are enabled in dataset config and run through ingest/transform/load.
- Census gov finance rows land in a dedicated compatibility table.
- Serving views exist and return annual state ratios:
  - `serving.v_state_federal_to_stategov_gdp_annual`
  - `serving.v_state_federal_to_persons_gdp_annual`
- Existing BEA/Census population paths remain backward compatible.
- Tests cover config parsing and source-specific transform/projection behavior.

## Constraints

- No new credentials or secrets.
- Preserve idempotent checkpoint/hash behavior and deterministic upserts.
- Keep changes additive; do not break existing compatibility tables/views.

## Non-goals

- Do not attempt a single fully reconciled cross-system "total federal received" dollar metric.
- Do not add IRS/BLS integration in this feature.
