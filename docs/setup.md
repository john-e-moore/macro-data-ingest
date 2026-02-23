# Setup Guide

## Prerequisites

- Python 3.11+
- AWS account with permissions to create:
  - S3 buckets and bucket policies
  - IAM roles/policies
  - RDS Postgres instance
  - Security groups
  - CloudWatch log groups
  - Optional SNS topics
- BEA API key
- Network path to Postgres (for load steps)

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
- `S3_DATA_BUCKET=tlg-macro-data`
- `S3_PREFIX_ROOT=staging`
- `BEA_API_KEY=example`
- `BEA_DATASET=NIPA`
- `BEA_TABLE_NAME=SQPCE`
- `BEA_FREQUENCY=A`
- `PG_HOST=example.rds.amazonaws.com`
- `PG_PORT=5432`
- `PG_DATABASE=macro`
- `PG_USER=macro_writer`
- `PG_PASSWORD=example`
- `PG_SCHEMA_GOLD=gold`
- `PG_SCHEMA_META=meta`
- `LOG_LEVEL=INFO`
- `SNS_ALERT_TOPIC_ARN=` (optional)

## Provisioning AWS Resources

Run from the repository root:

- `python scripts/provision_aws.py --env staging`
- `python scripts/provision_aws.py --env prod`

Expected provisioning outputs:
- Bucket/prefix strategy
- IAM role names or ARNs
- RDS endpoint and database name
- CloudWatch log group names
- Optional SNS topic ARN

Copy these outputs into:
- local `.env`
- GitHub repository secrets for workflows

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

## Rollback / Cleanup Guidance (Manual)

If provisioning created incorrect resources:
1. Disable workflow schedules to prevent accidental execution.
2. Remove created IAM policies/roles after detaching attachments.
3. Delete test S3 prefixes or buckets (after clearing objects/versions).
4. Delete non-production RDS instance and security groups.
5. Remove CloudWatch log groups and optional SNS topics.

Keep cleanup scripts explicit and environment-scoped to avoid production accidents.
