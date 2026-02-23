#!/usr/bin/env python
from __future__ import annotations

import argparse
from dataclasses import dataclass

from macro_data_ingest.config import load_config


@dataclass(frozen=True)
class ProvisionSummary:
    env: str
    data_bucket: str
    rds_identifier: str
    gha_role_name: str
    runtime_role_name: str
    cloudwatch_log_group: str
    sns_topic_arn: str | None


def provision(env: str) -> ProvisionSummary:
    """Provision AWS resources for a target environment.

    This is a scaffold implementation that validates shape and output format.
    Resource creation calls (boto3) will be added in a dedicated vertical slice.
    """
    cfg = load_config()
    suffix = env

    data_bucket = cfg.s3_data_bucket or f"tlg-macro-data-{suffix}"
    rds_identifier = f"tlg-macro-pg-{suffix}"
    gha_role_name = f"tlg-macro-gha-{suffix}"
    runtime_role_name = f"tlg-macro-runtime-{suffix}"
    cloudwatch_log_group = f"/tlg/macro-data-ingest/{suffix}"

    return ProvisionSummary(
        env=env,
        data_bucket=data_bucket,
        rds_identifier=rds_identifier,
        gha_role_name=gha_role_name,
        runtime_role_name=runtime_role_name,
        cloudwatch_log_group=cloudwatch_log_group,
        sns_topic_arn=cfg.sns_alert_topic_arn,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision AWS resources for macro-data-ingest.")
    parser.add_argument("--env", required=True, choices=["staging", "prod"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = provision(args.env)

    print("Provisioning summary (scaffold mode)")
    print(f"Environment: {result.env}")
    print(f"S3 data bucket: {result.data_bucket}")
    print(f"RDS identifier: {result.rds_identifier}")
    print(f"GitHub Actions role: {result.gha_role_name}")
    print(f"Pipeline runtime role: {result.runtime_role_name}")
    print(f"CloudWatch log group: {result.cloudwatch_log_group}")
    print(f"SNS topic ARN: {result.sns_topic_arn or '(not configured)'}")
    print("")
    print("Copy these values into .env and repository secrets as appropriate.")
    print("Rollback guidance: delete environment-tagged resources manually in reverse dependency order.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
