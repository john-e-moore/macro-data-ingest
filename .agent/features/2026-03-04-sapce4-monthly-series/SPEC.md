# Feature Brief: SAPCE4 Monthly Series

## Scope

- Add one new BEA dataset configuration for SAPCE4 at monthly grain.
- Extend Silver/Gold/load contracts so monthly periods are modeled and loaded safely.
- Preserve annual behavior and backward compatibility for existing annual serving contracts.

## Acceptance Criteria

1. `config/datasets.yaml` includes an enabled monthly SAPCE4 dataset spec.
2. Silver transform parses annual and monthly `TimePeriod` values into stable period keys.
3. Gold/conformed projection carries frequency/period attributes through load.
4. Postgres load supports monthly compatibility table upserts and conformed period dimensions.
5. Tests pass and staging probe verifies whether BEA returns monthly rows for SAPCE4.
6. Operator docs reflect monthly dataset availability and annual-serving scope boundaries.

## Constraints

- Keep Bronze immutability and payload-hash change detection unchanged.
- Maintain idempotent load behavior (monthly key uniqueness at period grain).
- Do not break existing annual dataset defaults.

## Non-Goals

- No new monthly serving views in this change.
- No schema migration automation for already-provisioned databases.

## Rollout / Rollback

- Rollout: merge to `main`, confirm a monthly-capable BEA table, then enable/validate in staging and prod.
- Rollback: disable monthly dataset spec entry or revert branch/PR if needed.
