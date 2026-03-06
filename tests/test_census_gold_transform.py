import pandas as pd

from macro_data_ingest.transforms.census_gold import (
    to_conformed_population_observation_frame,
    to_census_gold_frame,
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
