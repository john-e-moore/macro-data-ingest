from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd

from macro_data_ingest.config import AppConfig
from macro_data_ingest.datasets import BeaDatasetSpec, CensusDatasetSpec, DatasetSpec
from macro_data_ingest.run_metadata import utc_now_iso
from macro_data_ingest.transforms.census_silver import (
    to_census_silver_frame,
    validate_census_silver_frame,
)
from macro_data_ingest.transforms.silver import to_silver_frame, validate_silver_frame

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class TransformResult:
    run_id: str
    row_count: int
    source_payload_uri: str
    silver_uri: str
    manifest_uri: str


def _find_latest_payload_key(
    s3_client: Any, bucket: str, prefix_root: str, source: str, dataset: str
) -> str:
    prefix = f"{prefix_root}/bronze/{source}/{dataset}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    latest: dict[str, Any] | None = None

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("payload.json"):
                continue
            if latest is None or item["LastModified"] > latest["LastModified"]:
                latest = item

    if latest is None:
        raise ValueError(
            f"No Bronze payload found in s3://{bucket}/{prefix}. Run ingest first."
        )
    return latest["Key"]


def _read_json_from_s3(s3_client: Any, bucket: str, key: str) -> dict[str, Any]:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def _write_parquet_to_s3(s3_client: Any, bucket: str, key: str, frame: pd.DataFrame) -> str:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return f"s3://{bucket}/{key}"


def _write_json_to_s3(s3_client: Any, bucket: str, key: str, payload: dict[str, Any]) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def run_transform(
    config: AppConfig,
    run_id: str,
    dataset_spec: DatasetSpec,
    smoke: bool = False,
) -> TransformResult:
    del smoke  # reserved for future parameterized transforms
    if not config.s3_data_bucket:
        raise ValueError("S3_DATA_BUCKET is required for transform.")

    source = dataset_spec.source
    dataset = dataset_spec.dataset_id
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    s3 = boto3.client("s3", region_name=config.aws_region)
    payload_key = _find_latest_payload_key(
        s3_client=s3,
        bucket=config.s3_data_bucket,
        prefix_root=config.s3_prefix_root,
        source=source,
        dataset=dataset,
    )
    payload = _read_json_from_s3(s3, config.s3_data_bucket, payload_key)

    if isinstance(dataset_spec, BeaDatasetSpec):
        silver_frame = to_silver_frame(
            payload,
            bea_table_name=dataset_spec.bea_table_name,
            bea_frequency=dataset_spec.bea_frequency,
        )
        validate_silver_frame(silver_frame)
    elif isinstance(dataset_spec, CensusDatasetSpec):
        silver_frame = to_census_silver_frame(payload, dataset_spec)
        validate_census_silver_frame(silver_frame)
    else:
        raise ValueError(f"Unsupported dataset source for transform: {dataset_spec.source}")

    silver_key = (
        f"{config.s3_prefix_root}/silver/{source}/{dataset}/"
        f"extract_date={extract_date}/run_id={run_id}/part-000.parquet"
    )
    silver_uri = _write_parquet_to_s3(s3, config.s3_data_bucket, silver_key, silver_frame)
    LOGGER.info("wrote silver parquet", extra={"run_id": run_id, "stage": "transform"})

    manifest = {
        "run_id": run_id,
        "stage": "transform",
        "source": source,
        "dataset": dataset,
        "dataset_id": dataset_spec.dataset_id,
        "storage_dataset": dataset_spec.storage_dataset,
        "extracted_at_utc": utc_now_iso(),
        "input_payload_uri": f"s3://{config.s3_data_bucket}/{payload_key}",
        "row_count": int(len(silver_frame)),
        "quality_checks": {
            "non_null_required_columns": True,
            "primary_key_uniqueness": True,
            "row_count_gt_zero": True,
        },
        "output_partitions": [silver_uri],
    }
    if isinstance(dataset_spec, BeaDatasetSpec):
        manifest["bea_table_name"] = dataset_spec.bea_table_name
    if isinstance(dataset_spec, CensusDatasetSpec):
        manifest["census_dataset_path"] = dataset_spec.census_dataset_path
        manifest["census_variable"] = dataset_spec.census_variable
        manifest["census_series_kind"] = dataset_spec.census_series_kind
    manifest_key = (
        f"{config.s3_prefix_root}/silver/{source}/{dataset}/"
        f"extract_date={extract_date}/run_id={run_id}/manifest.json"
    )
    manifest_uri = _write_json_to_s3(s3, config.s3_data_bucket, manifest_key, manifest)

    return TransformResult(
        run_id=run_id,
        row_count=int(len(silver_frame)),
        source_payload_uri=f"s3://{config.s3_data_bucket}/{payload_key}",
        silver_uri=silver_uri,
        manifest_uri=manifest_uri,
    )
