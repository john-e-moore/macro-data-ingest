# Roadmap

## MVP

- Documentation and repository scaffolding
- AWS provisioning script (staging and prod support)
- Ingestion path: BEA API -> Bronze raw payload + manifest
- Basic change detection using payload hash/checkpoint
- Silver normalization for core keys
- Gold modeled table + Postgres load
- CI checks and daily workflow skeleton

## v1

- Hardened data quality checks with clear failure classes
- Backfill CLI support (date ranges and categories)
- Postgres serving views for YoY/MoM
- Improved observability (stage metrics and run outcomes)
- Optional SNS alerts on pipeline failures

## v2

- Additional macro sources (Census, BLS, IRS)
- Broader semantic model and cross-source joins
- Performance tuning for larger backfills
- Optional catalog/metadata integration and richer lineage reporting
