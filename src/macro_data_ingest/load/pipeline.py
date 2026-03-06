from __future__ import annotations

import io
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd

from macro_data_ingest.config import AppConfig
from macro_data_ingest.datasets import BeaDatasetSpec, CensusDatasetSpec, DatasetSpec
from macro_data_ingest.load.postgres_loader import PostgresLoader
from macro_data_ingest.run_metadata import utc_now_iso
from macro_data_ingest.transforms.census_gold import (
    to_conformed_state_gov_finance_observation_frame,
    to_census_gold_frame,
    to_census_state_gov_finance_gold_frame,
    to_conformed_population_observation_frame,
)
from macro_data_ingest.transforms.gold import to_conformed_observation_frame, to_gold_frame

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    run_id: str
    row_count: int
    source_silver_uri: str
    gold_table: str
    manifest_uri: str


def _find_latest_silver_key(
    s3_client: Any, bucket: str, prefix_root: str, source: str, dataset: str
) -> str:
    prefix = f"{prefix_root}/silver/{source}/{dataset}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    latest: dict[str, Any] | None = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".parquet"):
                continue
            if latest is None or item["LastModified"] > latest["LastModified"]:
                latest = item

    if latest is None:
        raise ValueError(f"No Silver parquet found in s3://{bucket}/{prefix}. Run transform first.")
    return latest["Key"]


def _read_parquet_from_s3(s3_client: Any, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_manifest_to_s3(
    s3_client: Any, bucket: str, key: str, payload: dict[str, Any]
) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def _build_dsn(config: AppConfig) -> str:
    if not config.pg_host:
        raise ValueError("PG_HOST is required for load.")
    if not config.pg_user or not config.pg_password:
        raise ValueError("PG_USER and PG_PASSWORD are required for load.")
    return (
        f"postgresql+psycopg://{config.pg_user}:{config.pg_password}"
        f"@{config.pg_host}:{config.pg_port}/{config.pg_database}"
    )


def _extract_vintage_tag_from_silver_key(silver_key: str) -> str:
    match = re.search(r"extract_date=(\d{4}-\d{2}-\d{2})", silver_key)
    if not match:
        raise ValueError(f"Could not derive vintage tag from Silver key: {silver_key}")
    return match.group(1)


def run_load(
    config: AppConfig,
    run_id: str,
    dataset_spec: DatasetSpec,
    smoke: bool = False,
) -> LoadResult:
    del smoke  # reserved for future load variations
    if not config.s3_data_bucket:
        raise ValueError("S3_DATA_BUCKET is required for load.")

    source = dataset_spec.source
    dataset = dataset_spec.dataset_id
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    s3 = boto3.client("s3", region_name=config.aws_region)

    silver_key = _find_latest_silver_key(
        s3_client=s3,
        bucket=config.s3_data_bucket,
        prefix_root=config.s3_prefix_root,
        source=source,
        dataset=dataset,
    )
    silver_frame = _read_parquet_from_s3(s3, config.s3_data_bucket, silver_key)
    vintage_tag = _extract_vintage_tag_from_silver_key(silver_key)
    if isinstance(dataset_spec, BeaDatasetSpec):
        gold_frame = to_gold_frame(silver_frame)
        if gold_frame.empty:
            raise ValueError("Gold frame is empty; cannot load to Postgres.")
        dataset_frequency = str(dataset_spec.bea_frequency).strip().upper()
        conformed_frame = to_conformed_observation_frame(
            gold_frame,
            source_name=dataset_spec.source,
            dataset_id=dataset_spec.dataset_id,
            vintage_tag=vintage_tag,
        )
        pk_cols = (
            ["bea_table_name", "state_fips", "period_code", "line_code"]
            if dataset_frequency == "M"
            else ["bea_table_name", "state_fips", "year", "line_code"]
        )
    elif isinstance(dataset_spec, CensusDatasetSpec):
        series_kind = dataset_spec.census_series_kind.strip().lower()
        if series_kind == "state_gov_finance":
            gold_frame = to_census_state_gov_finance_gold_frame(silver_frame)
            if gold_frame.empty:
                raise ValueError("Census state government finance Gold frame is empty; cannot load.")
            conformed_frame = to_conformed_state_gov_finance_observation_frame(
                gold_frame,
                source_name=dataset_spec.source,
                dataset_id=dataset_spec.dataset_id,
                vintage_tag=vintage_tag,
            )
            pk_cols = ["state_fips", "year", "census_variable", "census_agg_desc"]
        else:
            gold_frame = to_census_gold_frame(silver_frame)
            if gold_frame.empty:
                raise ValueError("Census Gold frame is empty; cannot load to Postgres.")
            conformed_frame = to_conformed_population_observation_frame(
                gold_frame,
                source_name=dataset_spec.source,
                dataset_id=dataset_spec.dataset_id,
                vintage_tag=vintage_tag,
            )
            pk_cols = ["state_fips", "year", "census_variable"]
    else:
        raise ValueError(f"Unsupported dataset source for load: {dataset_spec.source}")
    gold_frame["run_id"] = run_id

    loader = PostgresLoader(
        dsn=_build_dsn(config),
        schema_gold=config.pg_schema_gold,
        schema_meta=config.pg_schema_meta,
    )
    loader.ensure_base_objects()
    loader.upsert_gold_table(
        table_name=dataset_spec.target_table,
        frame=gold_frame,
        pk_cols=pk_cols,
    )
    loader.upsert_conformed_observations(
        conformed_frame=conformed_frame,
        run_id=run_id,
        source_release_tag=None,
    )
    loader.create_or_replace_views()
    loader.record_run(
        run_id=f"{run_id}:{dataset_spec.dataset_id}:load",
        stage="load",
        status="success",
        details={
            "pipeline_run_id": run_id,
            "dataset_id": dataset_spec.dataset_id,
            "storage_dataset": dataset_spec.storage_dataset,
            "source_silver_uri": f"s3://{config.s3_data_bucket}/{silver_key}",
            "row_count": int(len(gold_frame)),
            "gold_table": f"{config.pg_schema_gold}.{dataset_spec.target_table}",
            "loaded_at_utc": utc_now_iso(),
            "source": dataset_spec.source,
        },
    )
    LOGGER.info("loaded gold table into postgres", extra={"run_id": run_id, "stage": "load"})

    manifest = {
        "run_id": run_id,
        "stage": "load",
        "source": source,
        "dataset": dataset,
        "dataset_id": dataset_spec.dataset_id,
        "storage_dataset": dataset_spec.storage_dataset,
        "extracted_at_utc": utc_now_iso(),
        "input_silver_uri": f"s3://{config.s3_data_bucket}/{silver_key}",
        "row_count": int(len(gold_frame)),
        "target_table": f"{config.pg_schema_gold}.{dataset_spec.target_table}",
        "target_views": [
            "serving.obt_state_macro_annual_latest",
            "serving.v_macro_yoy",
            "serving.v_pce_state_yoy",
            "serving.v_pce_state_per_capita_annual",
            "serving.v_state_federal_to_stategov_gdp_annual",
            "serving.v_state_federal_to_persons_gdp_annual",
        ],
    }
    if isinstance(dataset_spec, BeaDatasetSpec):
        manifest["bea_table_name"] = dataset_spec.bea_table_name
    if isinstance(dataset_spec, CensusDatasetSpec):
        manifest["census_dataset_path"] = dataset_spec.census_dataset_path
        manifest["census_variable"] = dataset_spec.census_variable
        manifest["census_series_kind"] = dataset_spec.census_series_kind
    manifest_key = (
        f"{config.s3_prefix_root}/gold/{source}/{dataset}/"
        f"extract_date={extract_date}/run_id={run_id}/manifest.json"
    )
    manifest_uri = _write_manifest_to_s3(s3, config.s3_data_bucket, manifest_key, manifest)

    return LoadResult(
        run_id=run_id,
        row_count=int(len(gold_frame)),
        source_silver_uri=f"s3://{config.s3_data_bucket}/{silver_key}",
        gold_table=f"{config.pg_schema_gold}.{dataset_spec.target_table}",
        manifest_uri=manifest_uri,
    )
