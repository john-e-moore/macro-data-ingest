from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from macro_data_ingest.config import AppConfig


@dataclass(frozen=True)
class BaseDatasetSpec:
    dataset_id: str
    source: str
    storage_dataset: str
    enabled: bool


@dataclass(frozen=True)
class BeaDatasetSpec(BaseDatasetSpec):
    bea_dataset: str
    bea_table_name: str
    bea_frequency: str
    bea_start_year: int
    line_code: str
    geo_fips: str


@dataclass(frozen=True)
class CensusDatasetSpec(BaseDatasetSpec):
    census_dataset_path: str
    census_variable: str
    census_geography: str
    census_start_year: int
    census_frequency: str
    census_series_kind: str = "population"
    census_predicates: dict[str, str] | None = None
    census_measure_label: str = "Resident population"
    census_unit: str = "persons"


DatasetSpec = BeaDatasetSpec | CensusDatasetSpec


def _require_text(value: Any, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")
    return value.strip()


def _build_bea_spec(entry: dict[str, Any]) -> BeaDatasetSpec:
    line_code = _require_text(entry.get("line_code", "ALL"), "line_code").upper()
    if line_code == "":
        line_code = "ALL"
    return BeaDatasetSpec(
        dataset_id=_require_text(entry.get("dataset_id"), "dataset_id"),
        source=_require_text(entry.get("source", "bea"), "source"),
        storage_dataset=_require_text(entry.get("storage_dataset", "pce_state"), "storage_dataset"),
        enabled=bool(entry.get("enabled", True)),
        bea_dataset=_require_text(entry.get("bea_dataset", "Regional"), "bea_dataset"),
        bea_table_name=_require_text(entry.get("bea_table_name"), "bea_table_name"),
        bea_frequency=_require_text(entry.get("bea_frequency", "A"), "bea_frequency"),
        bea_start_year=int(entry.get("bea_start_year", 2000)),
        line_code=line_code,
        geo_fips=_require_text(entry.get("geo_fips", "STATE"), "geo_fips"),
    )


def _build_census_spec(entry: dict[str, Any]) -> CensusDatasetSpec:
    predicates_raw = entry.get("census_predicates")
    predicates: dict[str, str] | None = None
    if isinstance(predicates_raw, dict):
        predicates = {
            _require_text(key, "census_predicates key"): _require_text(
                value, f"census_predicates[{key}]"
            )
            for key, value in predicates_raw.items()
        }
    return CensusDatasetSpec(
        dataset_id=_require_text(entry.get("dataset_id"), "dataset_id"),
        source=_require_text(entry.get("source", "census"), "source"),
        storage_dataset=_require_text(
            entry.get("storage_dataset", "population_state"),
            "storage_dataset",
        ),
        enabled=bool(entry.get("enabled", True)),
        census_dataset_path=_require_text(
            entry.get("census_dataset_path", "acs/acs1"),
            "census_dataset_path",
        ),
        census_variable=_require_text(
            entry.get("census_variable", "B01003_001E"),
            "census_variable",
        ).upper(),
        census_geography=_require_text(entry.get("census_geography", "state"), "census_geography"),
        census_start_year=int(entry.get("census_start_year", 2000)),
        census_frequency=_require_text(entry.get("census_frequency", "A"), "census_frequency"),
        census_series_kind=_require_text(entry.get("census_series_kind", "population"), "census_series_kind"),
        census_predicates=predicates,
        census_measure_label=_require_text(
            entry.get("census_measure_label", "Resident population"), "census_measure_label"
        ),
        census_unit=_require_text(entry.get("census_unit", "persons"), "census_unit"),
    )


def _build_spec(entry: dict[str, Any]) -> DatasetSpec:
    source = _require_text(entry.get("source", "bea"), "source").strip().lower()
    if source == "bea":
        return _build_bea_spec(entry)
    if source == "census":
        return _build_census_spec(entry)
    raise ValueError(f"Unsupported dataset source: {source}")


def load_dataset_specs(config: AppConfig) -> list[DatasetSpec]:
    config_path = Path(config.datasets_config_path)
    if not config_path.exists():
        raise ValueError(f"Datasets config not found: {config.datasets_config_path}")

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

