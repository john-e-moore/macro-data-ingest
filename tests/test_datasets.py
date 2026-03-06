from pathlib import Path

import pytest

from macro_data_ingest.config import load_config
from macro_data_ingest.datasets import BeaDatasetSpec, CensusDatasetSpec, load_dataset_specs


def test_load_dataset_specs_from_yaml(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = tmp_path / "datasets.yaml"
    config_path.write_text(
        "\n".join(
            [
                "datasets:",
                "  - dataset_id: pce_state_sapce4",
                "    bea_table_name: SAPCE4",
                "    bea_dataset: Regional",
                "    bea_frequency: A",
                "    bea_start_year: 2000",
                "    line_code: ALL",
                "    geo_fips: STATE",
                "    target_table: pce_state_annual",
                "    enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATASETS_CONFIG_PATH", str(config_path))
    cfg = load_config()
    specs = load_dataset_specs(cfg)
    assert len(specs) == 1
    assert specs[0].dataset_id == "pce_state_sapce4"
    assert specs[0].bea_table_name == "SAPCE4"


def test_load_dataset_specs_legacy_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATASETS_CONFIG_PATH", "config/not-real.yaml")
    monkeypatch.setenv("BEA_TABLE_NAME", "SAPCE4")
    cfg = load_config()
    specs = load_dataset_specs(cfg)
    assert len(specs) == 1
    assert specs[0].dataset_id == "pce_state_sapce4"


def test_load_dataset_specs_supports_census_source(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "datasets.yaml"
    config_path.write_text(
        "\n".join(
            [
                "datasets:",
                "  - dataset_id: census_state_population",
                "    source: census",
                "    storage_dataset: population_state",
                "    census_dataset_path: acs/acs1",
                "    census_variable: B01003_001E",
                "    census_geography: state",
                "    census_start_year: 2000",
                "    census_frequency: A",
                "    target_table: population_state_annual",
                "    enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATASETS_CONFIG_PATH", str(config_path))
    cfg = load_config()
    specs = load_dataset_specs(cfg)
    assert len(specs) == 1
    assert isinstance(specs[0], CensusDatasetSpec)
    assert specs[0].census_dataset_path == "acs/acs1"


def test_load_dataset_specs_supports_sarpp_and_sarpi_tables(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = tmp_path / "datasets.yaml"
    config_path.write_text(
        "\n".join(
            [
                "datasets:",
                "  - dataset_id: state_regional_price_parities_sarpp",
                "    source: bea",
                "    storage_dataset: pce_state",
                "    bea_dataset: Regional",
                "    bea_table_name: SARPP",
                "    bea_frequency: A",
                "    bea_start_year: 2000",
                "    line_code: ALL",
                "    geo_fips: STATE",
                "    target_table: pce_state_annual",
                "    enabled: true",
                "  - dataset_id: state_real_income_and_pce_sarpi",
                "    source: bea",
                "    storage_dataset: pce_state",
                "    bea_dataset: Regional",
                "    bea_table_name: SARPI",
                "    bea_frequency: A",
                "    bea_start_year: 2000",
                "    line_code: ALL",
                "    geo_fips: STATE",
                "    target_table: pce_state_annual",
                "    enabled: true",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATASETS_CONFIG_PATH", str(config_path))
    cfg = load_config()
    specs = load_dataset_specs(cfg)
    assert [spec.dataset_id for spec in specs] == [
        "state_regional_price_parities_sarpp",
        "state_real_income_and_pce_sarpi",
    ]
    assert all(isinstance(spec, BeaDatasetSpec) for spec in specs)
    assert specs[0].bea_table_name == "SARPP"
    assert specs[1].bea_table_name == "SARPI"
    assert specs[0].bea_start_year == 2000
    assert specs[1].bea_start_year == 2000
