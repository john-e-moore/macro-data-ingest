# Setup Guide

## Prerequisites

- Python 3.10+
- AWS account with permissions to create:
  - S3 buckets and bucket policies
  - IAM roles/policies
  - RDS Postgres instance
  - Security groups
  - CloudWatch log groups
  - Optional SNS topics
- BEA API key
- Network path to Postgres (for load steps)
  - If RDS is private (recommended), local runs require VPN/bastion/port-forward access to the VPC.

## Local Environment Setup

1. Create and activate a virtual environment:
   - `python -m venv .venv`
   - `source .venv/bin/activate`
2. Install dependencies:
   - `pip install -e .[dev]`
3. Create local environment file:
   - `cp .env.template .env`
4. Fill required variables (see schema below).

## `.env` Schema (Example Values Only)

Do not commit real values.

- `APP_ENV=staging`
- `AWS_REGION=us-east-1`
- `AWS_ACCOUNT_ID=123456789012`
- `AWS_ACCESS_KEY_ID=example`
- `AWS_SECRET_ACCESS_KEY=example`
- `AWS_VPC_ID=vpc-xxxxxxxx`
- `AWS_PRIVATE_SUBNET_IDS=subnet-aaaa,subnet-bbbb`
- `AWS_ALLOWED_CIDRS=10.0.0.0/16`
- `S3_DATA_BUCKET=tlg-macro-data`
- `S3_PREFIX_ROOT=staging`
- `S3_LOG_BUCKET=tlg-macro-data-access-logs`
- `GITHUB_REPO=your-org/macro-data-ingest`
- `BEA_API_KEY=example`
- `BEA_DATASET=Regional`
- `BEA_TABLE_NAME=SAPCE3`
- `BEA_FREQUENCY=A`
- `BEA_START_YEAR=2000`
- `DATASETS_CONFIG_PATH=config/datasets.yaml`
- `PG_HOST=example.rds.amazonaws.com`
- `PG_PORT=5432`
- `PG_DATABASE=macro`
- `PG_USER=macro_writer`
- `PG_PASSWORD=example`
- `PG_INSTANCE_CLASS=db.t4g.micro`
- `PG_ALLOCATED_STORAGE=20`
- `PG_SCHEMA_GOLD=gold`
- `PG_SCHEMA_META=meta`
- `LOG_LEVEL=INFO`
- `SNS_ALERT_TOPIC_ARN=` (optional)

## Provisioning AWS Resources

Run from the repository root:

1. Plan only (safe, no resource creation):
   - `python scripts/provision_aws.py --env staging`
2. Apply resources once credentials/network values are set:
   - `python scripts/provision_aws.py --env staging --apply`
   - `python scripts/provision_aws.py --env prod --apply`

Expected provisioning outputs:
- Bucket/prefix strategy
- IAM role names or ARNs
- RDS endpoint and database name
- CloudWatch log group names
- Optional SNS topic ARN

Copy these outputs into:
- local `.env`
- GitHub repository secrets for workflows
- environment-specific deployment notes

## GitHub Actions Secrets

Set repository-level secrets:

- `AWS_REGION`
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `BEA_API_KEY`
- `PG_HOST`
- `PG_PORT`
- `PG_DATABASE`
- `PG_USER`
- `PG_PASSWORD`
- `S3_DATA_BUCKET`
- `S3_PREFIX_ROOT`
- `SNS_ALERT_TOPIC_ARN` (optional)

Recommended optional secrets (override defaults when needed):

- `BEA_DATASET` (default `Regional`)
- `BEA_TABLE_NAME` (default `SAPCE3`)
- `BEA_FREQUENCY` (default `A`)
- `BEA_START_YEAR` (default `2000`; ingestion requests this year through current year)

`config/datasets.yaml` is the canonical source for multi-table daily runs. By default it includes
`SAPCE3` and `SAPCE4` with `line_code: ALL`, so all SAPCE4 functions are ingested on each run.

## Rollback / Cleanup Guidance (Manual)

If provisioning created incorrect resources:
1. Disable workflow schedules to prevent accidental execution.
2. Remove created IAM policies/roles after detaching attachments.
3. Delete test S3 prefixes or buckets (after clearing objects/versions).
4. Delete non-production RDS instance and security groups.
5. Remove CloudWatch log groups and optional SNS topics.

Keep cleanup scripts explicit and environment-scoped to avoid production accidents.
