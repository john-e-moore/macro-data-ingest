import pandas as pd

from macro_data_ingest.transforms.gold import to_gold_frame


def test_to_gold_frame_projects_expected_columns() -> None:
    silver = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "year": 2024,
                "bea_table_name": "SAPCE3",
                "line_code": "1",
                "series_code": "SAPCE3-1",
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
    assert "bea_table_name" in gold.columns
    assert gold.iloc[0]["pce_value"] == 100.0
    assert gold.iloc[0]["pce_value_scaled"] == 100000000.0
