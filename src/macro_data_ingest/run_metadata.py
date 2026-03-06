from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from json import dumps
from typing import Any


@dataclass(frozen=True)
class RunManifest:
    run_id: str
    stage: str
    source: str
    dataset: str
    extracted_at_utc: str
    request_params: dict[str, Any]
    row_count: int | None = None
    payload_hash: str | None = None
    output_partitions: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_rows_hash(rows: list[dict[str, Any]]) -> str:
    """Compute a deterministic content hash for BEA row payloads.

    Source payloads include volatile metadata fields (for example, production timestamps)
    that should not trigger downstream reprocessing when row-level business content is
    unchanged.
    """
    normalized_rows = [dict(sorted(row.items())) for row in rows]
    normalized_rows.sort(
        key=lambda row: (
            str(row.get("Code", "")),
            str(row.get("GeoFips", "")),
            str(row.get("TimePeriod", "")),
        )
    )
    encoded = dumps(normalized_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


def stable_records_hash(records: list[dict[str, Any]]) -> str:
    """Compute deterministic hash for generic record lists."""
    normalized_records = [dict(sorted(record.items())) for record in records]
    normalized_records.sort(
        key=lambda record: dumps(record, sort_keys=True, separators=(",", ":"))
    )
    encoded = dumps(normalized_records, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()
