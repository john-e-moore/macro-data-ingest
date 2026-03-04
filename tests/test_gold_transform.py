import pandas as pd

from macro_data_ingest.transforms.gold import to_conformed_observation_frame, to_gold_frame


def test_to_gold_frame_projects_expected_columns() -> None:
    silver = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "A",
                "period_code": "2024",
                "year": 2024,
                "month": None,
                "quarter": None,
                "bea_table_name": "SAPCE3",
                "line_code": "1",
                "series_code": "SAPCE3-1",
                "series_name": "Total personal consumption expenditures",
                "function_name": "Personal consumption expenditures",
                "value": 100.0,
                "unit": "Millions of current dollars",
                "unit_mult": 6,
                "note_ref": "",
            }
        ]
    )

    gold = to_gold_frame(silver)
    assert "pce_value" in gold.columns
    assert "pce_value_scaled" in gold.columns
    assert "series_name" in gold.columns
    assert "function_name" in gold.columns
    assert "bea_table_name" in gold.columns
    assert gold.iloc[0]["frequency"] == "A"
    assert gold.iloc[0]["period_code"] == "2024"
    assert gold.columns.get_loc("series_name") == gold.columns.get_loc("series_code") + 1
    assert gold.columns.get_loc("function_name") == gold.columns.get_loc("series_name") + 1
    assert gold.iloc[0]["pce_value"] == 100.0
    assert gold.iloc[0]["pce_value_scaled"] == 100000000.0


def test_to_conformed_observation_frame_projects_conformed_keys() -> None:
    gold = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "frequency": "M",
                "period_code": "2024M02",
                "year": 2024,
                "month": 2,
                "quarter": None,
                "bea_table_name": "SAPCE4",
                "line_code": "1",
                "series_code": "SAPCE4-1",
                "series_name": "Total personal consumption expenditures",
                "function_name": "Personal consumption expenditures",
                "pce_value": 100.0,
                "pce_value_scaled": 100000000.0,
                "unit": "Millions of current dollars",
                "unit_mult": 6,
                "note_ref": "",
            }
        ]
    )
    conformed = to_conformed_observation_frame(
        gold,
        source_name="bea",
        dataset_id="pce_state_sapce4",
        vintage_tag="2026-03-04",
    )
    assert conformed.iloc[0]["source_name"] == "BEA"
    assert conformed.iloc[0]["dataset_id"] == "pce_state_sapce4"
    assert conformed.iloc[0]["frequency"] == "M"
    assert conformed.iloc[0]["period_code"] == "2024M02"
    assert conformed.iloc[0]["month"] == 2
    assert conformed.iloc[0]["vintage_tag"] == "2026-03-04"
