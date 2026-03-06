from __future__ import annotations

import pandas as pd


def _table_name_from_dataset_path(dataset_path: str) -> str:
    return dataset_path.strip().upper().replace("/", "_")


def _series_code(table_name: str, variable: str) -> str:
    return f"{table_name}-{variable.strip().upper()}"


def to_census_gold_frame(silver_frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "frequency",
        "period_code",
        "year",
        "population",
        "census_dataset_path",
        "census_variable",
        "census_series_kind",
        "census_measure_label",
        "unit",
        "note_ref",
    ]
    missing = [col for col in required if col not in silver_frame.columns]
    if missing:
        raise ValueError(f"Census silver frame missing required columns: {missing}")

    gold = silver_frame[required].copy()
    gold["year"] = pd.to_numeric(gold["year"], errors="coerce").astype("Int64")
    gold["population"] = pd.to_numeric(gold["population"], errors="coerce")
    gold = gold.sort_values(["period_code", "state_fips"]).reset_index(drop=True)
    return gold


def to_conformed_population_observation_frame(
    gold_frame: pd.DataFrame,
    *,
    source_name: str,
    dataset_id: str,
    vintage_tag: str,
) -> pd.DataFrame:
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "frequency",
        "period_code",
        "year",
        "population",
        "census_dataset_path",
        "census_variable",
        "unit",
        "note_ref",
    ]
    missing = [col for col in required if col not in gold_frame.columns]
    if missing:
        raise ValueError(f"Census gold frame missing required columns: {missing}")

    conformed = gold_frame[required].copy()
    conformed["source_name"] = source_name.strip().upper()
    conformed["dataset_id"] = dataset_id.strip()
    conformed["vintage_tag"] = vintage_tag.strip()
    conformed["month"] = pd.Series([pd.NA] * len(conformed), dtype="Int64")
    conformed["quarter"] = pd.Series([pd.NA] * len(conformed), dtype="Int64")
    table_name = conformed["census_dataset_path"].astype(str).map(_table_name_from_dataset_path)
    variable = conformed["census_variable"].astype(str).str.upper()
    conformed["bea_table_name"] = table_name
    conformed["line_code"] = variable
    conformed["series_code"] = [
        _series_code(tbl, var) for tbl, var in zip(table_name.tolist(), variable.tolist(), strict=False)
    ]
    conformed["series_name"] = "Resident population"
    conformed["function_name"] = "Resident population"
    conformed["raw_description"] = "Resident population"
    conformed["hierarchy_path_json"] = '["Resident population"]'
    conformed["unit_mult"] = 0
    conformed["pce_value"] = conformed["population"]
    conformed["pce_value_scaled"] = conformed["population"]
    conformed = conformed[
        [
            "source_name",
            "dataset_id",
            "bea_table_name",
            "frequency",
            "period_code",
            "year",
            "month",
            "quarter",
            "state_fips",
            "state_abbrev",
            "geo_name",
            "line_code",
            "series_code",
            "series_name",
            "function_name",
            "raw_description",
            "hierarchy_path_json",
            "unit",
            "unit_mult",
            "vintage_tag",
            "pce_value",
            "pce_value_scaled",
            "note_ref",
        ]
    ]
    return conformed


def to_census_state_gov_finance_gold_frame(silver_frame: pd.DataFrame) -> pd.DataFrame:
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "frequency",
        "period_code",
        "year",
        "amount",
        "census_dataset_path",
        "census_variable",
        "census_series_kind",
        "census_measure_label",
        "census_agg_desc",
        "unit",
        "note_ref",
    ]
    missing = [col for col in required if col not in silver_frame.columns]
    if missing:
        raise ValueError(f"Census silver frame missing required columns: {missing}")

    gold = silver_frame[required].copy()
    gold["year"] = pd.to_numeric(gold["year"], errors="coerce").astype("Int64")
    gold["amount"] = pd.to_numeric(gold["amount"], errors="coerce")
    gold = gold.sort_values(["period_code", "state_fips"]).reset_index(drop=True)
    return gold


def to_conformed_state_gov_finance_observation_frame(
    gold_frame: pd.DataFrame,
    *,
    source_name: str,
    dataset_id: str,
    vintage_tag: str,
) -> pd.DataFrame:
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "frequency",
        "period_code",
        "year",
        "amount",
        "census_dataset_path",
        "census_variable",
        "census_measure_label",
        "census_agg_desc",
        "unit",
        "note_ref",
    ]
    missing = [col for col in required if col not in gold_frame.columns]
    if missing:
        raise ValueError(f"Census gold frame missing required columns: {missing}")

    conformed = gold_frame[required].copy()
    conformed["source_name"] = source_name.strip().upper()
    conformed["dataset_id"] = dataset_id.strip()
    conformed["vintage_tag"] = vintage_tag.strip()
    conformed["month"] = pd.Series([pd.NA] * len(conformed), dtype="Int64")
    conformed["quarter"] = pd.Series([pd.NA] * len(conformed), dtype="Int64")
    table_name = conformed["census_dataset_path"].astype(str).map(_table_name_from_dataset_path)
    variable = conformed["census_variable"].astype(str).str.upper()
    agg_desc = conformed["census_agg_desc"].astype(str).str.upper()
    conformed["bea_table_name"] = table_name
    conformed["line_code"] = variable
    conformed["series_code"] = [
        f"{tbl}-{var}-{agg}"
        for tbl, var, agg in zip(table_name.tolist(), variable.tolist(), agg_desc.tolist(), strict=False)
    ]
    conformed["series_name"] = conformed["census_measure_label"].astype(str)
    conformed["function_name"] = conformed["census_measure_label"].astype(str)
    conformed["raw_description"] = conformed["census_measure_label"].astype(str)
    conformed["hierarchy_path_json"] = (
        '["' + conformed["census_measure_label"].astype(str).str.replace('"', "'", regex=False) + '"]'
    )
    # Census gov finance values are reported in thousands of dollars.
    conformed["unit_mult"] = 3
    conformed["pce_value"] = conformed["amount"]
    conformed["pce_value_scaled"] = conformed["amount"] * 1000.0
    conformed = conformed[
        [
            "source_name",
            "dataset_id",
            "bea_table_name",
            "frequency",
            "period_code",
            "year",
            "month",
            "quarter",
            "state_fips",
            "state_abbrev",
            "geo_name",
            "line_code",
            "series_code",
            "series_name",
            "function_name",
            "raw_description",
            "hierarchy_path_json",
            "unit",
            "unit_mult",
            "vintage_tag",
            "pce_value",
            "pce_value_scaled",
            "note_ref",
        ]
    ]
    return conformed
