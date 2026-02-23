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


def stable_payload_hash(payload: dict[str, Any]) -> str:
    encoded = dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()
