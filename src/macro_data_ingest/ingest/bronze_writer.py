from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.exceptions import ClientError


class BronzeWriter:
    """Bronze and metadata writer for S3."""

    def __init__(self, bucket: str, prefix_root: str, aws_region: str, s3_client: Any | None = None) -> None:
        self.bucket = bucket
        self.prefix_root = prefix_root
        self.s3 = s3_client or boto3.client("s3", region_name=aws_region)

    def _root(self, source: str, dataset: str) -> str:
        return f"{self.prefix_root}/bronze/{source}/{dataset}"

    def _raw_key(self, source: str, dataset: str, extract_date: str, run_id: str) -> str:
        return (
            f"{self._root(source, dataset)}"
            f"/extract_date={extract_date}/run_id={run_id}/payload.json"
        )

    def _manifest_key(self, source: str, dataset: str, extract_date: str, run_id: str) -> str:
        return (
            f"{self._root(source, dataset)}"
            f"/extract_date={extract_date}/run_id={run_id}/manifest.json"
        )

    def _checkpoint_key(self, source: str, dataset: str) -> str:
        return f"{self._root(source, dataset)}/checkpoints/latest.json"

    def write_raw_payload(
        self,
        source: str,
        dataset: str,
        extract_date: str,
        run_id: str,
        payload: dict[str, Any],
    ) -> str:
        key = self._raw_key(source, dataset, extract_date, run_id)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(payload, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{key}"

    def write_manifest(
        self,
        source: str,
        dataset: str,
        extract_date: str,
        run_id: str,
        manifest: dict[str, Any],
    ) -> str:
        key = self._manifest_key(source, dataset, extract_date, run_id)
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(manifest, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{key}"

    def read_latest_checkpoint(self, source: str, dataset: str) -> dict[str, Any] | None:
        key = self._checkpoint_key(source, dataset)
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in {"NoSuchKey", "404"}:
                return None
            raise
        raw = response["Body"].read().decode("utf-8")
        return json.loads(raw)

    def write_checkpoint(
        self,
        source: str,
        dataset: str,
        payload_hash: str,
        run_id: str,
        extracted_at_utc: str,
        requested_year_range: str,
        previous_payload_hash: str | None = None,
        source_release_tag: str | None = None,
    ) -> str:
        key = self._checkpoint_key(source, dataset)
        body = {
            "payload_hash": payload_hash,
            "previous_payload_hash": previous_payload_hash,
            "run_id": run_id,
            "extracted_at_utc": extracted_at_utc,
            "requested_year_range": requested_year_range,
            "source_release_tag": source_release_tag,
        }
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(body, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return f"s3://{self.bucket}/{key}"
