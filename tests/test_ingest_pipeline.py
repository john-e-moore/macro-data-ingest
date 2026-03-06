from datetime import datetime, timezone

import pytest

from macro_data_ingest.datasets import CensusDatasetSpec
from macro_data_ingest.ingest.bea_client import BeaQuery
from macro_data_ingest.ingest.pipeline import (
    _build_census_ingest_payload,
    _fetch_payload,
    _is_changed,
    _source_release_tag,
    _validate_requested_frequency,
    _year_range,
)
from macro_data_ingest.run_metadata import stable_rows_hash


def test_is_changed_when_no_checkpoint() -> None:
    assert _is_changed(None, "abc")


def test_is_changed_when_hash_matches() -> None:
    checkpoint = {"payload_hash": "abc"}
    assert not _is_changed(checkpoint, "abc")


def test_is_changed_when_hash_differs() -> None:
    checkpoint = {"payload_hash": "abc"}
    assert _is_changed(checkpoint, "def")


def test_year_range_starts_at_configured_year() -> None:
    current_year = datetime.now(timezone.utc).year
    year_range = _year_range(2000)
    assert year_range.startswith("2000,2001")
    assert year_range.endswith(str(current_year))


def test_year_range_rejects_future_year() -> None:
    current_year = datetime.now(timezone.utc).year
    with pytest.raises(ValueError):
        _year_range(current_year + 1)


def test_source_release_tag_prefers_release_date() -> None:
    payload = {"BEAAPI": {"Results": {"ReleaseDate": "2026-03-01", "ReleaseName": "Ignored"}}}
    assert _source_release_tag(payload) == "2026-03-01"


def test_source_release_tag_falls_back_to_none() -> None:
    payload = {"BEAAPI": {"Results": {"Data": []}}}
    assert _source_release_tag(payload) is None


def test_stable_rows_hash_ignores_row_order() -> None:
    rows_a = [
        {"Code": "SAPCE3-1", "GeoFips": "01000", "TimePeriod": "2000", "DataValue": "1"},
        {"Code": "SAPCE3-1", "GeoFips": "02000", "TimePeriod": "2000", "DataValue": "2"},
    ]
    rows_b = [rows_a[1], rows_a[0]]
    assert stable_rows_hash(rows_a) == stable_rows_hash(rows_b)


def test_fetch_payload_expands_all_line_codes() -> None:
    class DummyClient:
        def fetch_line_code_descriptions(self, dataset: str, table_name: str) -> dict[str, str]:
            assert dataset == "Regional"
            assert table_name == "SAPCE4"
            return {"1": "Total PCE", "10": "Food services"}

        def fetch(self, query: BeaQuery) -> dict:
            return {
                "BEAAPI": {
                    "Results": {
                        "Data": [
                            {"Code": f"SAPCE4-{query.line_code}", "GeoFips": "01000", "TimePeriod": "2024"}
                        ]
                    }
                }
            }

        @staticmethod
        def extract_rows(payload: dict) -> list[dict]:
            return payload["BEAAPI"]["Results"]["Data"]

    query = BeaQuery(dataset="Regional", table_name="SAPCE4", year="2024", line_code="ALL")
    payload, used_query, rows = _fetch_payload(DummyClient(), query, smoke=False)
    assert used_query.line_code == "ALL"
    assert len(rows) == 2
    assert len(payload["BEAAPI"]["Results"]["Data"]) == 2
    assert rows[0]["FunctionName"] == "Total PCE"
    assert rows[1]["FunctionName"] == "Food services"


def test_fetch_payload_populates_function_name_for_single_line_code() -> None:
    class DummyClient:
        def fetch_line_code_descriptions(self, dataset: str, table_name: str) -> dict[str, str]:
            assert dataset == "Regional"
            assert table_name == "SAPCE4"
            return {"1": "Total PCE"}

        def fetch(self, query: BeaQuery) -> dict:
            assert query.line_code == "1"
            return {
                "BEAAPI": {
                    "Results": {
                        "Data": [
                            {"Code": "SAPCE4-1", "GeoFips": "01000", "TimePeriod": "2024"}
                        ]
                    }
                }
            }

        @staticmethod
        def extract_rows(payload: dict) -> list[dict]:
            return payload["BEAAPI"]["Results"]["Data"]

    query = BeaQuery(dataset="Regional", table_name="SAPCE4", year="2024", line_code="1")
    payload, used_query, rows = _fetch_payload(DummyClient(), query, smoke=False)
    assert used_query.line_code == "1"
    assert len(rows) == 1
    assert len(payload["BEAAPI"]["Results"]["Data"]) == 1
    assert rows[0]["FunctionName"] == "Total PCE"


def test_validate_requested_frequency_accepts_matching_monthly_rows() -> None:
    rows = [{"TimePeriod": "2024M01"}, {"TimePeriod": "2024M02"}]
    _validate_requested_frequency(rows, "M")


def test_validate_requested_frequency_rejects_mismatched_frequency() -> None:
    rows = [{"TimePeriod": "2024"}, {"TimePeriod": "2023"}]
    with pytest.raises(ValueError):
        _validate_requested_frequency(rows, "M")


def test_build_census_ingest_payload_backfills_pre_2005_for_acs(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyConfig:
        census_api_key = "x"

    spec = CensusDatasetSpec(
        dataset_id="census_state_population",
        source="census",
        storage_dataset="population_state",
        target_table="population_state_annual",
        enabled=True,
        census_dataset_path="acs/acs1",
        census_variable="B01003_001E",
        census_geography="state",
        census_start_year=2000,
        census_frequency="A",
    )

    class DummyCensusClient:
        def __init__(self, api_key: str) -> None:
            assert api_key == "x"

        @staticmethod
        def fetch_state_population(*, years: list[int], dataset_path: str, variable: str) -> list[dict]:
            assert dataset_path == "acs/acs1"
            assert variable == "B01003_001E"
            assert 2005 in years
            assert 2000 not in years
            return [{"NAME": "Alabama", "B01003_001E": "4569805", "state": "01", "YEAR": "2005"}]

        @staticmethod
        def fetch_state_population_intercensal(
            *, years: list[int], variable_alias: str
        ) -> list[dict]:
            assert variable_alias == "B01003_001E"
            assert years == [2000, 2001, 2002, 2003, 2004]
            return [{"NAME": "Alabama", "B01003_001E": "4452173", "state": "01", "YEAR": "2000"}]

    monkeypatch.setattr("macro_data_ingest.ingest.pipeline.CensusClient", DummyCensusClient)
    _, rows, _, request_params, _ = _build_census_ingest_payload(
        config=DummyConfig(),
        dataset_spec=spec,
        smoke=False,
    )
    assert len(rows) == 2
    assert rows[0]["YEAR"] == "2000"
    assert rows[1]["YEAR"] == "2005"
    assert request_params["dataset_path"] == "acs/acs1"
