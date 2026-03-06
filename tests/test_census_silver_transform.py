import pandas as pd
import pytest

from macro_data_ingest.datasets import CensusDatasetSpec
from macro_data_ingest.transforms.census_silver import (
    to_census_silver_frame,
    validate_census_silver_frame,
)


def _spec() -> CensusDatasetSpec:
    return CensusDatasetSpec(
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


def _state_gov_finance_spec() -> CensusDatasetSpec:
    return CensusDatasetSpec(
        dataset_id="census_state_gov_finance_federal_intergovernmental_revenue",
        source="census",
        storage_dataset="state_gov_finance",
        target_table="state_gov_finance_annual",
        enabled=True,
        census_dataset_path="timeseries/govs",
        census_variable="AMOUNT",
        census_geography="state",
        census_start_year=2012,
        census_frequency="A",
        census_series_kind="state_gov_finance",
        census_predicates={"SVY_COMP": "02", "GOVTYPE": "002", "AGG_DESC": "SF0004"},
        census_measure_label="Federal intergovernmental revenue",
        census_unit="dollars_thousands",
    )


def test_to_census_silver_frame_maps_columns() -> None:
    payload = {
        "CENSUSAPI": {
            "Results": {
                "Data": [
                    {"NAME": "Alabama", "B01003_001E": "5108468", "state": "01", "YEAR": "2024"},
                    {"NAME": "Alaska", "B01003_001E": "733406", "state": "02", "YEAR": "2024"},
                ]
            }
        }
    }
    silver = to_census_silver_frame(payload, _spec())
    assert len(silver) == 2
    assert silver.iloc[0]["frequency"] == "A"
    assert silver.iloc[0]["period_code"] == "2024"
    assert silver.iloc[0]["state_fips"] == "01"
    assert silver.iloc[0]["state_abbrev"] == "AL"
    assert silver.iloc[0]["population"] == 5108468


def test_validate_census_silver_frame_rejects_duplicates() -> None:
    frame = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "A",
                "period_code": "2024",
                "year": 2024,
                "population": 1.0,
                "census_dataset_path": "acs/acs1",
                "census_variable": "B01003_001E",
                "census_series_kind": "population",
                "unit": "persons",
                "note_ref": "",
            },
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "A",
                "period_code": "2024",
                "year": 2024,
                "population": 2.0,
                "census_dataset_path": "acs/acs1",
                "census_variable": "B01003_001E",
                "census_series_kind": "population",
                "unit": "persons",
                "note_ref": "",
            },
        ]
    )
    with pytest.raises(ValueError):
        validate_census_silver_frame(frame)


def test_to_census_silver_frame_maps_state_gov_finance_rows() -> None:
    payload = {
        "CENSUSAPI": {
            "Results": {
                "Data": [
                    {"NAME": "Alabama", "AMOUNT": "17107053", "state": "01", "YEAR": "2023"},
                ]
            }
        }
    }
    silver = to_census_silver_frame(payload, _state_gov_finance_spec())
    assert len(silver) == 1
    assert silver.iloc[0]["amount"] == 17107053
    assert silver.iloc[0]["census_series_kind"] == "state_gov_finance"
    assert silver.iloc[0]["census_agg_desc"] == "SF0004"
