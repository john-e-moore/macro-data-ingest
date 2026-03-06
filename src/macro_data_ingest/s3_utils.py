from __future__ import annotations

import io
import json
from typing import Any

import pandas as pd


def find_latest_object_key(
    s3_client: Any,
    *,
    bucket: str,
    prefix: str,
    suffix: str,
) -> str:
    paginator = s3_client.get_paginator("list_objects_v2")
    latest: dict[str, Any] | None = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith(suffix):
                continue
            if latest is None or item["LastModified"] > latest["LastModified"]:
                latest = item
    if latest is None:
        raise ValueError(f"No object found in s3://{bucket}/{prefix} with suffix '{suffix}'.")
    return latest["Key"]


def read_json_from_s3(s3_client: Any, *, bucket: str, key: str) -> dict[str, Any]:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def write_json_to_s3(s3_client: Any, *, bucket: str, key: str, payload: dict[str, Any]) -> str:
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )
    return f"s3://{bucket}/{key}"


def read_parquet_from_s3(s3_client: Any, *, bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def write_parquet_to_s3(s3_client: Any, *, bucket: str, key: str, frame: pd.DataFrame) -> str:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    return f"s3://{bucket}/{key}"
