from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3

from macro_data_ingest.config import AppConfig
from macro_data_ingest.datasets import BeaDatasetSpec, CensusDatasetSpec, DatasetSpec
from macro_data_ingest.load.postgres_loader import PostgresLoader
from macro_data_ingest.run_metadata import utc_now_iso
from macro_data_ingest.s3_utils import (
    find_latest_object_key,
    read_parquet_from_s3,
    write_json_to_s3,
)
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
    conformed_table: str
    manifest_uri: str

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

    silver_key = find_latest_object_key(
        s3,
        bucket=config.s3_data_bucket,
        prefix=f"{config.s3_prefix_root}/silver/{source}/{dataset}/",
        suffix=".parquet",
    )
    silver_frame = read_parquet_from_s3(s3, bucket=config.s3_data_bucket, key=silver_key)
    vintage_tag = _extract_vintage_tag_from_silver_key(silver_key)
    if isinstance(dataset_spec, BeaDatasetSpec):
        gold_frame = to_gold_frame(silver_frame)
        if gold_frame.empty:
            raise ValueError("Gold frame is empty; cannot load to Postgres.")
        conformed_frame = to_conformed_observation_frame(
            gold_frame,
            source_name=dataset_spec.source,
            dataset_id=dataset_spec.dataset_id,
            vintage_tag=vintage_tag,
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
    else:
        raise ValueError(f"Unsupported dataset source for load: {dataset_spec.source}")
    loader = PostgresLoader(
        dsn=_build_dsn(config),
        schema_gold=config.pg_schema_gold,
        schema_meta=config.pg_schema_meta,
    )
    loader.ensure_base_objects()
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
            "conformed_table": f"{config.pg_schema_gold}.fact_macro_observation",
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
        "conformed_table": f"{config.pg_schema_gold}.fact_macro_observation",
        "target_views": [
            "serving.obt_state_macro_annual_latest",
            "serving.v_macro_yoy",
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
    manifest_uri = write_json_to_s3(
        s3,
        bucket=config.s3_data_bucket,
        key=manifest_key,
        payload=manifest,
    )

    return LoadResult(
        run_id=run_id,
        row_count=int(len(gold_frame)),
        source_silver_uri=f"s3://{config.s3_data_bucket}/{silver_key}",
        conformed_table=f"{config.pg_schema_gold}.fact_macro_observation",
        manifest_uri=manifest_uri,
    )
