from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    log_level: str
    aws_region: str
    aws_account_id: str
    aws_vpc_id: str
    aws_private_subnet_ids: str
    aws_allowed_cidrs: str
    s3_data_bucket: str
    s3_log_bucket: str
    s3_prefix_root: str
    github_repo: str
    bea_api_key: str
    bea_dataset: str
    bea_table_name: str
    bea_frequency: str
    bea_start_year: int
    pg_host: str
    pg_port: int
    pg_database: str
    pg_user: str
    pg_password: str
    pg_instance_class: str
    pg_allocated_storage: int
    pg_schema_gold: str
    pg_schema_meta: str
    sns_alert_topic_arn: str | None


def load_config() -> AppConfig:
    load_dotenv()

    return AppConfig(
        app_env=os.getenv("APP_ENV", "staging"),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        aws_account_id=os.getenv("AWS_ACCOUNT_ID", ""),
        aws_vpc_id=os.getenv("AWS_VPC_ID", ""),
        aws_private_subnet_ids=os.getenv("AWS_PRIVATE_SUBNET_IDS", ""),
        aws_allowed_cidrs=os.getenv("AWS_ALLOWED_CIDRS", "10.0.0.0/16"),
        s3_data_bucket=os.getenv("S3_DATA_BUCKET", ""),
        s3_log_bucket=os.getenv("S3_LOG_BUCKET", ""),
        s3_prefix_root=os.getenv("S3_PREFIX_ROOT", "staging"),
        github_repo=os.getenv("GITHUB_REPO", ""),
        bea_api_key=os.getenv("BEA_API_KEY", ""),
        bea_dataset=os.getenv("BEA_DATASET", "Regional"),
        bea_table_name=os.getenv("BEA_TABLE_NAME", "SAPCE3"),
        bea_frequency=os.getenv("BEA_FREQUENCY", "A"),
        bea_start_year=int(os.getenv("BEA_START_YEAR", "2000")),
        pg_host=os.getenv("PG_HOST", ""),
        pg_port=int(os.getenv("PG_PORT", "5432")),
        pg_database=os.getenv("PG_DATABASE", "macro"),
        pg_user=os.getenv("PG_USER", ""),
        pg_password=os.getenv("PG_PASSWORD", ""),
        pg_instance_class=os.getenv("PG_INSTANCE_CLASS", "db.t4g.micro"),
        pg_allocated_storage=int(os.getenv("PG_ALLOCATED_STORAGE", "20")),
        pg_schema_gold=os.getenv("PG_SCHEMA_GOLD", "gold"),
        pg_schema_meta=os.getenv("PG_SCHEMA_META", "meta"),
        sns_alert_topic_arn=os.getenv("SNS_ALERT_TOPIC_ARN") or None,
    )
