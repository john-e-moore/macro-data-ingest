# Roadmap

## MVP

- Documentation and repository scaffolding
- AWS provisioning script (staging and prod support)
- Ingestion path: BEA API -> Bronze raw payload + manifest
- Basic change detection using payload hash/checkpoint
- Silver normalization for core keys
- Gold conformed dimensions/fact + Postgres load
- Serving OBT/YoY view layer for analyst/API queries
- CI checks and daily workflow skeleton

## v1

- Hardened data quality checks with clear failure classes
- Backfill CLI support (date ranges and categories)
- Postgres serving views for YoY/MoM
- Additional serving contracts (materialized OBTs) for high-volume endpoints
- Improved observability (stage metrics and run outcomes)
- Optional SNS alerts on pipeline failures

## v2

- Additional macro sources (BLS, IRS)
- Broader semantic model and cross-source joins
- Performance tuning for larger backfills
- Optional catalog/metadata integration and richer lineage reporting
