#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.exceptions import ClientError

from macro_data_ingest.config import load_config


@dataclass(frozen=True)
class ProvisionPlan:
    env: str
    aws_account_id: str
    aws_region: str
    data_bucket: str
    log_bucket: str
    bronze_retention_days: int
    logs_retention_days: int
    rds_identifier: str
    rds_subnet_group: str
    rds_security_group_name: str
    rds_instance_class: str
    rds_allocated_storage: int
    gha_role_name: str
    runtime_role_name: str
    cloudwatch_log_group: str
    sns_topic_name: str
    github_repo: str


@dataclass(frozen=True)
class ProvisionResult:
    plan: ProvisionPlan
    sns_topic_arn: str | None
    actions: list[str]


def _csv_to_list(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def build_plan(env: str) -> ProvisionPlan:
    cfg = load_config()
    account_id = cfg.aws_account_id or "unset-account-id"

    base_name = f"tlg-macro-{env}"
    data_bucket = cfg.s3_data_bucket or f"{base_name}-data-{account_id}-{cfg.aws_region}"
    log_bucket = cfg.s3_log_bucket or f"{base_name}-access-logs-{account_id}-{cfg.aws_region}"

    return ProvisionPlan(
        env=env,
        aws_account_id=account_id,
        aws_region=cfg.aws_region,
        data_bucket=data_bucket,
        log_bucket=log_bucket,
        bronze_retention_days=3650,
        logs_retention_days=90,
        rds_identifier=f"{base_name}-pg",
        rds_subnet_group=f"{base_name}-db-subnets",
        rds_security_group_name=f"{base_name}-rds-sg",
        rds_instance_class=cfg.pg_instance_class,
        rds_allocated_storage=cfg.pg_allocated_storage,
        gha_role_name=f"{base_name}-gha-role",
        runtime_role_name=f"{base_name}-runtime-role",
        cloudwatch_log_group=f"/tlg/macro-data-ingest/{env}",
        sns_topic_name=f"{base_name}-alerts",
        github_repo=cfg.github_repo,
    )


class AwsProvisioner:
    def __init__(self, plan: ProvisionPlan) -> None:
        self.plan = plan
        self.cfg = load_config()
        self.session = boto3.session.Session(region_name=plan.aws_region)
        self.s3 = self.session.client("s3")
        self.iam = self.session.client("iam")
        self.logs = self.session.client("logs")
        self.sns = self.session.client("sns")
        self.ec2 = self.session.client("ec2")
        self.rds = self.session.client("rds")
        self.sts = self.session.client("sts")
        self.actions: list[str] = []

    def ensure_account_id(self) -> str:
        if self.plan.aws_account_id != "unset-account-id":
            return self.plan.aws_account_id
        identity = self.sts.get_caller_identity()
        return identity["Account"]

    def _bucket_exists(self, bucket_name: str) -> bool:
        try:
            self.s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            return False

    def ensure_bucket(self, bucket_name: str) -> None:
        if not self._bucket_exists(bucket_name):
            kwargs: dict[str, Any] = {"Bucket": bucket_name}
            if self.plan.aws_region != "us-east-1":
                kwargs["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.plan.aws_region,
                }
            self.s3.create_bucket(**kwargs)
            self.actions.append(f"created s3 bucket: {bucket_name}")
        else:
            self.actions.append(f"reused s3 bucket: {bucket_name}")

        self.s3.put_public_access_block(
            Bucket=bucket_name,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        self.s3.put_bucket_versioning(
            Bucket=bucket_name,
            VersioningConfiguration={"Status": "Enabled"},
        )
        self.s3.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {
                        "ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"},
                    }
                ]
            },
        )
        self.actions.append(f"applied bucket security controls: {bucket_name}")

    def ensure_lifecycle_rules(self) -> None:
        self.s3.put_bucket_lifecycle_configuration(
            Bucket=self.plan.data_bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "expire-run-logs",
                        "Status": "Enabled",
                        "Prefix": f"{self.plan.env}/logs/",
                        "Expiration": {"Days": self.plan.logs_retention_days},
                    }
                ]
            },
        )
        self.actions.append("applied lifecycle policy for run logs")

    def ensure_log_group(self) -> None:
        existing = self.logs.describe_log_groups(
            logGroupNamePrefix=self.plan.cloudwatch_log_group
        ).get("logGroups", [])
        if any(item["logGroupName"] == self.plan.cloudwatch_log_group for item in existing):
            self.actions.append(f"reused cloudwatch log group: {self.plan.cloudwatch_log_group}")
            return

        self.logs.create_log_group(logGroupName=self.plan.cloudwatch_log_group)
        self.actions.append(f"created cloudwatch log group: {self.plan.cloudwatch_log_group}")

    def ensure_sns_topic(self) -> str:
        topic_arn = self.cfg.sns_alert_topic_arn
        if topic_arn:
            self.actions.append(f"reused sns topic from config: {topic_arn}")
            return topic_arn

        response = self.sns.create_topic(Name=self.plan.sns_topic_name)
        topic_arn = response["TopicArn"]
        self.actions.append(f"created sns topic: {topic_arn}")
        return topic_arn

    def ensure_iam_roles(self, account_id: str) -> None:
        oidc_provider_arn = (
            f"arn:aws:iam::{account_id}:oidc-provider/token.actions.githubusercontent.com"
        )
        repo_scope = f"repo:{self.plan.github_repo}:*" if self.plan.github_repo else "repo:*/*:*"

        gha_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Federated": oidc_provider_arn},
                    "Action": "sts:AssumeRoleWithWebIdentity",
                    "Condition": {
                        "StringEquals": {"token.actions.githubusercontent.com:aud": "sts.amazonaws.com"},
                        "StringLike": {"token.actions.githubusercontent.com:sub": repo_scope},
                    },
                }
            ],
        }
        runtime_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": f"arn:aws:iam::{account_id}:root"},
                    "Action": "sts:AssumeRole",
                    "Condition": {
                        "ArnLike": {
                            "aws:PrincipalArn": f"arn:aws:iam::{account_id}:role/{self.plan.gha_role_name}"
                        }
                    },
                }
            ],
        }
        runtime_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "S3DataAccess",
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:ListBucket",
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.plan.data_bucket}",
                        f"arn:aws:s3:::{self.plan.data_bucket}/*",
                    ],
                },
                {
                    "Sid": "CloudWatchWrite",
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                        "logs:DescribeLogStreams",
                    ],
                    "Resource": f"arn:aws:logs:{self.plan.aws_region}:{account_id}:log-group:{self.plan.cloudwatch_log_group}:*",
                },
                {
                    "Sid": "RDSMetadata",
                    "Effect": "Allow",
                    "Action": [
                        "rds:DescribeDBInstances",
                        "rds:DescribeDBSubnetGroups",
                    ],
                    "Resource": "*",
                },
            ],
        }

        self._ensure_role(self.plan.gha_role_name, gha_trust_policy)
        self._ensure_role(self.plan.runtime_role_name, runtime_trust_policy)
        self.iam.put_role_policy(
            RoleName=self.plan.runtime_role_name,
            PolicyName=f"{self.plan.runtime_role_name}-inline",
            PolicyDocument=json.dumps(runtime_policy),
        )
        self.actions.append(f"applied inline policy to role: {self.plan.runtime_role_name}")

    def _ensure_role(self, role_name: str, trust_policy: dict[str, Any]) -> None:
        try:
            self.iam.get_role(RoleName=role_name)
            self.actions.append(f"reused iam role: {role_name}")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "NoSuchEntity":
                raise
            self.iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=f"macro-data-ingest role for {self.plan.env}",
            )
            self.actions.append(f"created iam role: {role_name}")

    def ensure_rds(self) -> None:
        private_subnets = _csv_to_list(self.cfg.aws_private_subnet_ids)
        allowed_cidrs = _csv_to_list(self.cfg.aws_allowed_cidrs)
        if not self.cfg.aws_vpc_id or not private_subnets:
            raise ValueError(
                "AWS_VPC_ID and AWS_PRIVATE_SUBNET_IDS must be set to provision RDS resources."
            )
        if not self.cfg.pg_password:
            raise ValueError("PG_PASSWORD must be set to provision the RDS instance.")

        sg_id = self._ensure_rds_security_group(self.cfg.aws_vpc_id, allowed_cidrs)
        self._ensure_subnet_group(private_subnets)
        self._ensure_rds_instance(sg_id)

    def _ensure_rds_security_group(self, vpc_id: str, allowed_cidrs: list[str]) -> str:
        existing = self.ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [self.plan.rds_security_group_name]},
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        )["SecurityGroups"]
        if existing:
            sg_id = existing[0]["GroupId"]
            self.actions.append(f"reused security group: {sg_id}")
        else:
            created = self.ec2.create_security_group(
                GroupName=self.plan.rds_security_group_name,
                Description=f"RDS access for macro-data-ingest {self.plan.env}",
                VpcId=vpc_id,
            )
            sg_id = created["GroupId"]
            self.actions.append(f"created security group: {sg_id}")

        for cidr in allowed_cidrs:
            try:
                self.ec2.authorize_security_group_ingress(
                    GroupId=sg_id,
                    IpPermissions=[
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 5432,
                            "ToPort": 5432,
                            "IpRanges": [{"CidrIp": cidr}],
                        }
                    ],
                )
                self.actions.append(f"authorized postgres ingress from: {cidr}")
            except ClientError as exc:
                if exc.response["Error"]["Code"] != "InvalidPermission.Duplicate":
                    raise
                self.actions.append(f"reused postgres ingress rule for: {cidr}")
        return sg_id

    def _ensure_subnet_group(self, private_subnets: list[str]) -> None:
        try:
            self.rds.describe_db_subnet_groups(DBSubnetGroupName=self.plan.rds_subnet_group)
            self.actions.append(f"reused rds subnet group: {self.plan.rds_subnet_group}")
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "DBSubnetGroupNotFoundFault":
                raise
            self.rds.create_db_subnet_group(
                DBSubnetGroupName=self.plan.rds_subnet_group,
                DBSubnetGroupDescription=f"macro-data-ingest {self.plan.env} db subnets",
                SubnetIds=private_subnets,
            )
            self.actions.append(f"created rds subnet group: {self.plan.rds_subnet_group}")

    def _ensure_rds_instance(self, security_group_id: str) -> None:
        try:
            self.rds.describe_db_instances(DBInstanceIdentifier=self.plan.rds_identifier)
            self.actions.append(f"reused rds instance: {self.plan.rds_identifier}")
            return
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "DBInstanceNotFound":
                raise

        self.rds.create_db_instance(
            DBInstanceIdentifier=self.plan.rds_identifier,
            Engine="postgres",
            MasterUsername=self.cfg.pg_user or "macro_admin",
            MasterUserPassword=self.cfg.pg_password,
            DBName=self.cfg.pg_database,
            DBInstanceClass=self.plan.rds_instance_class,
            AllocatedStorage=self.plan.rds_allocated_storage,
            PubliclyAccessible=False,
            StorageEncrypted=True,
            DeletionProtection=False,
            VpcSecurityGroupIds=[security_group_id],
            DBSubnetGroupName=self.plan.rds_subnet_group,
            BackupRetentionPeriod=7,
        )
        self.actions.append(f"created rds instance: {self.plan.rds_identifier}")

    def apply(self) -> ProvisionResult:
        account_id = self.ensure_account_id()
        self.ensure_bucket(self.plan.data_bucket)
        self.ensure_bucket(self.plan.log_bucket)
        self.ensure_lifecycle_rules()
        self.ensure_log_group()
        topic_arn = self.ensure_sns_topic()
        self.ensure_iam_roles(account_id)
        self.ensure_rds()

        return ProvisionResult(plan=self.plan, sns_topic_arn=topic_arn, actions=self.actions)


def provision_plan_only(env: str) -> ProvisionResult:
    cfg = load_config()
    plan = build_plan(env)
    actions = [
        "plan generated",
        "no resources created (run with --apply to provision)",
    ]
    topic_arn = cfg.sns_alert_topic_arn
    return ProvisionResult(plan=plan, sns_topic_arn=topic_arn, actions=actions)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Provision AWS resources for macro-data-ingest.")
    parser.add_argument("--env", required=True, choices=["staging", "prod"])
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply changes in AWS. Without this flag, script runs in plan-only mode.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.apply:
        plan = build_plan(args.env)
        result = AwsProvisioner(plan).apply()
        mode_label = "APPLY"
    else:
        result = provision_plan_only(args.env)
        mode_label = "PLAN"

    print(f"Provisioning summary ({mode_label} mode)")
    print(f"Environment: {result.plan.env}")
    print(f"AWS Region: {result.plan.aws_region}")
    print(f"AWS Account ID: {result.plan.aws_account_id}")
    print(f"S3 data bucket: {result.plan.data_bucket}")
    print(f"S3 access log bucket: {result.plan.log_bucket}")
    print(f"RDS identifier: {result.plan.rds_identifier}")
    print(f"RDS subnet group: {result.plan.rds_subnet_group}")
    print(f"RDS security group name: {result.plan.rds_security_group_name}")
    print(f"GitHub Actions role: {result.plan.gha_role_name}")
    print(f"Pipeline runtime role: {result.plan.runtime_role_name}")
    print(f"CloudWatch log group: {result.plan.cloudwatch_log_group}")
    print(f"SNS topic ARN: {result.sns_topic_arn or '(will be created on apply)'}")
    print("")
    print("Actions:")
    for action in result.actions:
        print(f"- {action}")
    print("")
    print("Copy these values into .env and repository secrets as appropriate.")
    print("Rollback guidance: delete environment-tagged resources manually in reverse dependency order.")
    if not args.apply:
        print("Next: fill .env and run with --apply when you are ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
