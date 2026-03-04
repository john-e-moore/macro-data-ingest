from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from macro_data_ingest.config import AppConfig
from macro_data_ingest.datasets import BeaDatasetSpec
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


def _line_code_from_series_code(series_code: str) -> str:
    if "-" in series_code:
        return series_code.split("-", maxsplit=1)[1]
    return series_code


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


def _bea_query(spec: BeaDatasetSpec, smoke: bool) -> BeaQuery:
    year = _smoke_year() if smoke else _year_range(spec.bea_start_year)
    return BeaQuery(
        dataset=spec.bea_dataset,
        table_name=spec.bea_table_name,
        frequency=spec.bea_frequency,
        year=year,
        geo_fips=spec.geo_fips,
        line_code=spec.line_code,
    )


def _fetch_payload(
    client: BeaClient, query: BeaQuery, smoke: bool
) -> tuple[dict[str, Any], BeaQuery, list[dict[str, Any]]]:
    if query.line_code.upper() == "ALL":
        line_code_descriptions = client.fetch_line_code_descriptions(query.dataset, query.table_name)
        line_codes = list(line_code_descriptions.keys())
        if smoke:
            line_codes = line_codes[:3]
        merged_rows: list[dict[str, Any]] = []
        base_payload: dict[str, Any] | None = None
        for line_code in line_codes:
            candidate = BeaQuery(
                dataset=query.dataset,
                table_name=query.table_name,
                frequency=query.frequency,
                year=query.year,
                geo_fips=query.geo_fips,
                line_code=line_code,
            )
            payload = client.fetch(candidate)
            rows = client.extract_rows(payload)
            for row in rows:
                row["FunctionName"] = line_code_descriptions.get(
                    _line_code_from_series_code(str(row.get("Code", line_code))),
                    "",
                )
            if base_payload is None:
                base_payload = payload
            merged_rows.extend(rows)
        if base_payload is None:
            raise ValueError("Failed to fetch any payloads for LineCode=ALL expansion.")
        merged_payload = dict(base_payload)
        merged_payload.setdefault("BEAAPI", {}).setdefault("Results", {})["Data"] = merged_rows
        return merged_payload, query, merged_rows

    if not smoke:
        payload = client.fetch(query)
        rows = client.extract_rows(payload)
        line_code_descriptions = client.fetch_line_code_descriptions(query.dataset, query.table_name)
        for row in rows:
            row["FunctionName"] = line_code_descriptions.get(
                _line_code_from_series_code(str(row.get("Code", query.line_code))),
                "",
            )
        return payload, query, rows

    base_year = int(_smoke_year())
    line_code_descriptions = client.fetch_line_code_descriptions(query.dataset, query.table_name)
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
        for row in rows:
            row["FunctionName"] = line_code_descriptions.get(
                _line_code_from_series_code(str(row.get("Code", query.line_code))),
                "",
            )
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


def _period_frequency(time_period: Any) -> str:
    value = str(time_period or "").strip().upper()
    if re.fullmatch(r"\d{4}M(0[1-9]|1[0-2])", value):
        return "M"
    if re.fullmatch(r"\d{4}Q[1-4]", value):
        return "Q"
    if re.fullmatch(r"\d{4}", value):
        return "A"
    return "UNKNOWN"


def _validate_requested_frequency(rows: list[dict[str, Any]], requested_frequency: str) -> None:
    normalized = requested_frequency.strip().upper()
    if normalized not in {"A", "M"}:
        return
    observed = {_period_frequency(row.get("TimePeriod")) for row in rows}
    if normalized in observed:
        return
    raise ValueError(
        "BEA response did not include the requested frequency "
        f"{normalized}. Observed frequencies={sorted(observed)}. "
        "Verify table/frequency availability in BEA metadata."
    )


def run_ingest(
    config: AppConfig,
    run_id: str,
    dataset_spec: BeaDatasetSpec,
    smoke: bool = False,
) -> IngestResult:
    if not config.bea_api_key:
        raise ValueError("BEA_API_KEY is required for ingest.")
    if not config.s3_data_bucket:
        raise ValueError("S3_DATA_BUCKET is required for ingest.")

    source = dataset_spec.source
    dataset = dataset_spec.dataset_id
    extract_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    query = _bea_query(dataset_spec, smoke=smoke)
    client = BeaClient(api_key=config.bea_api_key)
    payload, query, rows = _fetch_payload(client, query, smoke=smoke)
    if not rows:
        raise ValueError(
            "BEA returned zero rows for ingest query "
            f"dataset={query.dataset} table={query.table_name} year={query.year} "
            f"geo_fips={query.geo_fips} line_code={query.line_code}."
        )
    _validate_requested_frequency(rows, dataset_spec.bea_frequency)
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
    manifest["storage_dataset"] = dataset_spec.storage_dataset
    manifest["dataset_id"] = dataset_spec.dataset_id
    manifest["bea_table_name"] = dataset_spec.bea_table_name
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
