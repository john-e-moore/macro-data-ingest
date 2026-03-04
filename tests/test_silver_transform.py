import pandas as pd
import pytest

from macro_data_ingest.transforms.silver import to_silver_frame, validate_silver_frame


def _sample_payload() -> dict:
    return {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {
                        "GeoFips": "00000",
                        "GeoName": "United States *",
                        "TimePeriod": "2024",
                        "Code": "SAPCE3-1",
                        "FunctionName": "Personal consumption expenditures",
                        "DataValue": "1000.0",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "*",
                    },
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024",
                        "Code": "SAPCE3-1",
                        "FunctionName": "Personal consumption expenditures",
                        "DataValue": "123.4",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "",
                    },
                    {
                        "GeoFips": "91000",
                        "GeoName": "New England",
                        "TimePeriod": "2024",
                        "Code": "SAPCE3-1",
                        "FunctionName": "Personal consumption expenditures",
                        "DataValue": "456.7",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "",
                    },
                ]
            }
        }
    }


def test_to_silver_frame_keeps_only_state_rows() -> None:
    silver = to_silver_frame(_sample_payload())
    assert len(silver) == 1
    assert silver.iloc[0]["state_abbrev"] == "AL"
    assert silver.iloc[0]["line_code"] == "1"
    assert silver.iloc[0]["bea_table_name"] == "SAPCE3"
    assert silver.iloc[0]["series_name"] == ""
    assert silver.iloc[0]["function_name"] == "Personal consumption expenditures"


def test_validate_silver_frame_happy_path() -> None:
    silver = to_silver_frame(_sample_payload())
    validate_silver_frame(silver)


def test_validate_silver_frame_duplicate_pk_fails() -> None:
    frame = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "year": 2024,
                "bea_table_name": "SAPCE3",
                "line_code": "1",
                "series_code": "SAPCE3-1",
                "function_name": "Personal consumption expenditures",
                "value": 1.0,
                "unit": "u",
                "unit_mult": 6,
                "note_ref": "",
            },
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "geo_name": "Alabama",
                "year": 2024,
                "bea_table_name": "SAPCE3",
                "line_code": "1",
                "series_code": "SAPCE3-1",
                "function_name": "Personal consumption expenditures",
                "value": 2.0,
                "unit": "u",
                "unit_mult": 6,
                "note_ref": "",
            },
        ]
    )

    with pytest.raises(ValueError):
        validate_silver_frame(frame)


def test_to_silver_frame_allows_explicit_table_name() -> None:
    silver = to_silver_frame(_sample_payload(), bea_table_name="sapce4")
    assert set(silver["bea_table_name"].unique()) == {"SAPCE4"}


def test_to_silver_frame_splits_series_and_function_name() -> None:
    payload = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024",
                        "Code": "SAPCE4-90",
                        "FunctionName": "[SAPCE4] Total personal consumption expenditures: Life insurance",
                        "DataValue": "123.4",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "",
                    }
                ]
            }
        }
    }
    silver = to_silver_frame(payload, bea_table_name="SAPCE4")
    assert silver.iloc[0]["series_name"] == "Total personal consumption expenditures"
    assert silver.iloc[0]["function_name"] == "Life insurance"
