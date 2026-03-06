import pandas as pd

from macro_data_ingest.transforms.census_gold import (
    to_conformed_population_observation_frame,
    to_conformed_state_gov_finance_observation_frame,
    to_census_gold_frame,
    to_census_state_gov_finance_gold_frame,
)


def test_to_census_gold_frame_projects_expected_columns() -> None:
    silver = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "A",
                "period_code": "2024",
                "year": 2024,
                "population": 5108468,
                "census_dataset_path": "acs/acs1",
                "census_variable": "B01003_001E",
                "census_series_kind": "population",
                "census_measure_label": "Resident population",
                "unit": "persons",
                "note_ref": "",
            }
        ]
    )
    gold = to_census_gold_frame(silver)
    assert len(gold) == 1
    assert gold.iloc[0]["population"] == 5108468
    assert gold.iloc[0]["census_variable"] == "B01003_001E"


def test_to_conformed_population_observation_frame_maps_conformed_columns() -> None:
    gold = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "A",
                "period_code": "2024",
                "year": 2024,
                "population": 5108468,
                "census_dataset_path": "acs/acs1",
                "census_variable": "B01003_001E",
                "census_series_kind": "population",
                "census_measure_label": "Resident population",
                "unit": "persons",
                "note_ref": "",
            }
        ]
    )
    conformed = to_conformed_population_observation_frame(
        gold,
        source_name="census",
        dataset_id="census_state_population",
        vintage_tag="2026-03-06",
    )
    assert conformed.iloc[0]["source_name"] == "CENSUS"
    assert conformed.iloc[0]["series_code"] == "ACS_ACS1-B01003_001E"
    assert conformed.iloc[0]["pce_value"] == 5108468
    assert conformed.iloc[0]["unit_mult"] == 0


def test_state_gov_finance_gold_and_conformed_projection() -> None:
    silver = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "A",
                "period_code": "2023",
                "year": 2023,
                "amount": 17107053,
                "census_dataset_path": "timeseries/govs",
                "census_variable": "AMOUNT",
                "census_series_kind": "state_gov_finance",
                "census_measure_label": "Federal intergovernmental revenue",
                "census_agg_desc": "SF0004",
                "unit": "dollars_thousands",
                "note_ref": "",
            }
        ]
    )
    gold = to_census_state_gov_finance_gold_frame(silver)
    conformed = to_conformed_state_gov_finance_observation_frame(
        gold,
        source_name="census",
        dataset_id="census_state_gov_finance_federal_intergovernmental_revenue",
        vintage_tag="2026-03-06",
    )
    assert gold.iloc[0]["amount"] == 17107053
    assert conformed.iloc[0]["source_name"] == "CENSUS"
    assert conformed.iloc[0]["series_code"] == "TIMESERIES_GOVS-AMOUNT-SF0004"
    assert conformed.iloc[0]["pce_value"] == 17107053
    assert conformed.iloc[0]["pce_value_scaled"] == 17107053000
    assert conformed.iloc[0]["unit_mult"] == 3
