from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from macro_data_ingest.config import AppConfig


@dataclass(frozen=True)
class BeaDatasetSpec:
    dataset_id: str
    source: str
    storage_dataset: str
    bea_dataset: str
    bea_table_name: str
    bea_frequency: str
    bea_start_year: int
    line_code: str
    geo_fips: str
    target_table: str
    enabled: bool = True


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _build_spec(entry: dict[str, Any]) -> BeaDatasetSpec:
    line_code = _require_text(entry.get("line_code", "ALL"), "line_code").upper()
    if line_code == "":
        line_code = "ALL"
    return BeaDatasetSpec(
        dataset_id=_require_text(entry.get("dataset_id"), "dataset_id"),
        source=_require_text(entry.get("source", "bea"), "source"),
        storage_dataset=_require_text(entry.get("storage_dataset", "pce_state"), "storage_dataset"),
        bea_dataset=_require_text(entry.get("bea_dataset", "Regional"), "bea_dataset"),
        bea_table_name=_require_text(entry.get("bea_table_name"), "bea_table_name"),
        bea_frequency=_require_text(entry.get("bea_frequency", "A"), "bea_frequency"),
        bea_start_year=int(entry.get("bea_start_year", 2000)),
        line_code=line_code,
        geo_fips=_require_text(entry.get("geo_fips", "STATE"), "geo_fips"),
        target_table=_require_text(entry.get("target_table", "pce_state_annual"), "target_table"),
        enabled=bool(entry.get("enabled", True)),
    )


def _legacy_default_spec(config: AppConfig) -> list[BeaDatasetSpec]:
    table_name = config.bea_table_name.strip()
    dataset_id = f"pce_state_{table_name.lower()}"
    return [
        BeaDatasetSpec(
            dataset_id=dataset_id,
            source="bea",
            storage_dataset="pce_state",
            bea_dataset=config.bea_dataset,
            bea_table_name=table_name,
            bea_frequency=config.bea_frequency,
            bea_start_year=config.bea_start_year,
            line_code="ALL",
            geo_fips="STATE",
            target_table="pce_state_annual",
            enabled=True,
        )
    ]


def load_dataset_specs(config: AppConfig) -> list[BeaDatasetSpec]:
    config_path = Path(config.datasets_config_path)
    if not config_path.exists():
        return _legacy_default_spec(config)

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{config.datasets_config_path} must contain a top-level mapping.")
    entries = raw.get("datasets")
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"{config.datasets_config_path} must define a non-empty datasets list.")

    specs = [_build_spec(entry) for entry in entries if isinstance(entry, dict)]
    enabled_specs = [spec for spec in specs if spec.enabled]
    if not enabled_specs:
        raise ValueError("No enabled datasets found in datasets config.")
    return enabled_specs

