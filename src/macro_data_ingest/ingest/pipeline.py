from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from macro_data_ingest.config import AppConfig
from macro_data_ingest.ingest.bea_client import BeaClient, BeaQuery
from macro_data_ingest.ingest.bronze_writer import BronzeWriter
from macro_data_ingest.run_metadata import RunManifest, stable_rows_hash, utc_now_iso

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestResult:
    run_id: str
    changed: bool
    payload_hash: str
    row_count: int
    raw_payload_uri: str | None
    manifest_uri: str
    checkpoint_uri: str


def _smoke_year() -> str:
    return str(datetime.now(timezone.utc).year - 1)


def _year_range(start_year: int) -> str:
    current_year = datetime.now(timezone.utc).year
    if start_year > current_year:
        raise ValueError(
            f"BEA_START_YEAR={start_year} is after current year {current_year}."
        )
    if start_year < 1900:
        raise ValueError("BEA_START_YEAR must be >= 1900.")
    years = [str(year) for year in range(start_year, current_year + 1)]
    return ",".join(years)


def _bea_query(config: AppConfig, smoke: bool) -> BeaQuery:
    year = _smoke_year() if smoke else _year_range(config.bea_start_year)
    return BeaQuery(
        dataset=config.bea_dataset,
        table_name=config.bea_table_name,
        frequency=config.bea_frequency,
        year=year,
        geo_fips="STATE",
        line_code="1",
    )


def _fetch_payload(
    client: BeaClient, query: BeaQuery, smoke: bool
) -> tuple[dict[str, Any], BeaQuery, list[dict[str, Any]]]:
    if not smoke:
        payload = client.fetch(query)
        rows = client.extract_rows(payload)
        return payload, query, rows

    base_year = int(_smoke_year())
    for year in [str(base_year - offset) for offset in range(0, 5)]:
        candidate = BeaQuery(
            dataset=query.dataset,
            table_name=query.table_name,
            frequency=query.frequency,
            year=year,
            geo_fips=query.geo_fips,
            line_code=query.line_code,
        )
        payload = client.fetch(candidate)
        rows = client.extract_rows(payload)
        if rows:
            return payload, candidate, rows

    # Fall back to most recent attempt if no rows were returned.
    return payload, candidate, rows


def _is_changed(previous_checkpoint: dict[str, Any] | None, payload_hash: str) -> bool:
    if previous_checkpoint is None:
        return True
    return previous_checkpoint.get("payload_hash") != payload_hash


def _source_release_tag(payload: dict[str, Any]) -> str | None:
    results = payload.get("BEAAPI", {}).get("Results", {})
    for key in ["ReleaseDate", "ReleaseName", "Statistic"]:
        value = results.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def run_ingest(config: AppConfig, run_id: str, smoke: bool = False) -> IngestResult:
    if not config.bea_api_key:
        raise ValueError("BEA_API_KEY is required for ingest.")
    if not config.s3_data_bucket:
        raise ValueError("S3_DATA_BUCKET is required for ingest.")

    source = "bea"
    dataset = "pce_state"
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    query = _bea_query(config, smoke=smoke)
    client = BeaClient(api_key=config.bea_api_key)
    payload, query, rows = _fetch_payload(client, query, smoke=smoke)
    payload_hash = stable_rows_hash(rows)

    writer = BronzeWriter(
        bucket=config.s3_data_bucket,
        prefix_root=config.s3_prefix_root,
        aws_region=config.aws_region,
    )

    previous_checkpoint = writer.read_latest_checkpoint(source=source, dataset=dataset)
    changed = _is_changed(previous_checkpoint, payload_hash)

    raw_uri: str | None = None
    if changed:
        raw_uri = writer.write_raw_payload(
            source=source,
            dataset=dataset,
            extract_date=extract_date,
            run_id=run_id,
            payload=payload,
        )
        LOGGER.info(
            "wrote bronze raw payload",
            extra={"run_id": run_id, "stage": "ingest"},
        )
    else:
        LOGGER.info(
            "source payload unchanged; skipped raw payload write",
            extra={"run_id": run_id, "stage": "ingest"},
        )

    extracted_at_utc = utc_now_iso()
    previous_payload_hash = None
    if previous_checkpoint is not None:
        previous_payload_hash = previous_checkpoint.get("payload_hash")
    source_release_tag = _source_release_tag(payload)
    checkpoint_uri = writer.write_checkpoint(
        source=source,
        dataset=dataset,
        payload_hash=payload_hash,
        run_id=run_id,
        extracted_at_utc=extracted_at_utc,
        requested_year_range=query.year,
        previous_payload_hash=previous_payload_hash,
        source_release_tag=source_release_tag,
    )

    manifest = RunManifest(
        run_id=run_id,
        stage="ingest",
        source=source,
        dataset=dataset,
        extracted_at_utc=extracted_at_utc,
        request_params={
            "dataset": query.dataset,
            "table_name": query.table_name,
            "frequency": query.frequency,
            "year": query.year,
            "geo_fips": query.geo_fips,
            "line_code": query.line_code,
        },
        row_count=len(rows),
        payload_hash=payload_hash,
        output_partitions=[raw_uri] if raw_uri else [],
    ).to_dict()
    manifest["changed"] = changed
    manifest["checkpoint_uri"] = checkpoint_uri
    manifest["vintage"] = {
        "requested_year_range": query.year,
        "source_release_tag": source_release_tag,
        "previous_payload_hash": previous_payload_hash,
    }

    manifest_uri = writer.write_manifest(
        source=source,
        dataset=dataset,
        extract_date=extract_date,
        run_id=run_id,
        manifest=manifest,
    )

    return IngestResult(
        run_id=run_id,
        changed=changed,
        payload_hash=payload_hash,
        row_count=len(rows),
        raw_payload_uri=raw_uri,
        manifest_uri=manifest_uri,
        checkpoint_uri=checkpoint_uri,
    )
