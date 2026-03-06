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
    assert silver.iloc[0]["frequency"] == "A"
    assert silver.iloc[0]["period_code"] == "2024"
    assert silver.iloc[0]["line_code"] == "1"
    assert silver.iloc[0]["bea_table_name"] == "SAPCE3"
    assert silver.iloc[0]["series_name"] == ""
    assert silver.iloc[0]["function_name"] == "Personal consumption expenditures"
    assert silver.iloc[0]["raw_description"] == "Personal consumption expenditures"
    assert silver.iloc[0]["hierarchy_path_json"] == '["Personal consumption expenditures"]'


def test_validate_silver_frame_happy_path() -> None:
    silver = to_silver_frame(_sample_payload())
    validate_silver_frame(silver)


def test_validate_silver_frame_duplicate_pk_fails() -> None:
    frame = pd.DataFrame(
        [
            {
                "state_fips": "01",
                "state_abbrev": "AL",
                "frequency": "A",
                "period_code": "2024",
                "geo_name": "Alabama",
                "year": 2024,
                "month": None,
                "quarter": None,
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
                "frequency": "A",
                "period_code": "2024",
                "geo_name": "Alabama",
                "year": 2024,
                "month": None,
                "quarter": None,
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
    assert silver.iloc[0]["hierarchy_path_json"] == (
        '["Total personal consumption expenditures", "Life insurance"]'
    )


def test_to_silver_frame_parses_multi_level_hierarchy_path() -> None:
    payload = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024",
                        "Code": "SAPCE1-200",
                        "FunctionName": "[SAPCE1] Goods: Durable goods: Motor vehicles and parts",
                        "DataValue": "123.4",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "",
                    }
                ]
            }
        }
    }
    silver = to_silver_frame(payload, bea_table_name="SAPCE1")
    assert silver.iloc[0]["series_name"] == "Goods"
    assert silver.iloc[0]["function_name"] == "Motor vehicles and parts"
    assert silver.iloc[0]["hierarchy_path_json"] == (
        '["Goods", "Durable goods", "Motor vehicles and parts"]'
    )


def test_to_silver_frame_parses_monthly_period_code() -> None:
    payload = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024M02",
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
    assert silver.iloc[0]["frequency"] == "M"
    assert silver.iloc[0]["period_code"] == "2024M02"
    assert silver.iloc[0]["year"] == 2024
    assert silver.iloc[0]["month"] == 2


def test_to_silver_frame_filters_to_requested_frequency() -> None:
    payload = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024",
                        "Code": "SAPCE4-1",
                        "FunctionName": "Personal consumption expenditures",
                        "DataValue": "100",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "",
                    },
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024M02",
                        "Code": "SAPCE4-1",
                        "FunctionName": "Personal consumption expenditures",
                        "DataValue": "101",
                        "CL_UNIT": "Millions of current dollars",
                        "UNIT_MULT": "6",
                        "NoteRef": "",
                    },
                ]
            }
        }
    }
    silver = to_silver_frame(payload, bea_table_name="SAPCE4", bea_frequency="M")
    assert len(silver) == 1
    assert silver.iloc[0]["frequency"] == "M"
    assert silver.iloc[0]["period_code"] == "2024M02"


def test_to_silver_frame_handles_missing_note_ref_column() -> None:
    payload = {
        "BEAAPI": {
            "Results": {
                "Data": [
                    {
                        "GeoFips": "01000",
                        "GeoName": "Alabama",
                        "TimePeriod": "2024",
                        "Code": "SARPP-1",
                        "FunctionName": "[SARPP] RPPs: All items",
                        "DataValue": "100.0",
                        "CL_UNIT": "Percent",
                        "UNIT_MULT": "0",
                    }
                ]
            }
        }
    }
    silver = to_silver_frame(payload, bea_table_name="SARPP")
    assert len(silver) == 1
    assert silver.iloc[0]["note_ref"] == ""
