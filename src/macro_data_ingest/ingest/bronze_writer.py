from __future__ import annotations

from typing import Any


class BronzeWriter:
    """Scaffold for immutable Bronze writes to S3."""

    def __init__(self, bucket: str, prefix_root: str) -> None:
        self.bucket = bucket
        self.prefix_root = prefix_root

    def write_raw_payload(
        self,
        source: str,
        dataset: str,
        extract_date: str,
        payload: dict[str, Any],
    ) -> str:
        raise NotImplementedError("Bronze S3 write is not implemented yet.")

    def write_manifest(
        self,
        source: str,
        dataset: str,
        extract_date: str,
        manifest: dict[str, Any],
    ) -> str:
        raise NotImplementedError("Manifest write is not implemented yet.")
