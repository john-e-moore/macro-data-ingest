from __future__ import annotations

import pandas as pd


def to_gold_frame(silver_frame: pd.DataFrame) -> pd.DataFrame:
    """Produce analytics-ready Gold frame from Silver records."""
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "year",
        "bea_table_name",
        "line_code",
        "series_code",
        "series_name",
        "function_name",
        "value",
        "unit",
        "unit_mult",
        "note_ref",
    ]
    missing = [col for col in required if col not in silver_frame.columns]
    if missing:
        raise ValueError(f"Silver frame missing required columns: {missing}")

    gold = silver_frame[required].copy()
    gold["year"] = pd.to_numeric(gold["year"], errors="coerce").astype("Int64")
    gold["pce_value"] = pd.to_numeric(gold["value"], errors="coerce")
    gold["pce_value_scaled"] = gold["pce_value"] * (10 ** gold["unit_mult"])
    gold = gold.drop(columns=["value"])
    gold = gold.sort_values(["bea_table_name", "year", "state_fips", "line_code"]).reset_index(
        drop=True
    )
    return gold


def to_conformed_observation_frame(
    gold_frame: pd.DataFrame,
    *,
    source_name: str,
    dataset_id: str,
    vintage_tag: str,
) -> pd.DataFrame:
    """Project the Gold wide frame into conformed observation keys and measures."""
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "year",
        "bea_table_name",
        "line_code",
        "series_code",
        "series_name",
        "function_name",
        "pce_value",
        "pce_value_scaled",
        "unit",
        "unit_mult",
        "note_ref",
    ]
    missing = [col for col in required if col not in gold_frame.columns]
    if missing:
        raise ValueError(f"Gold frame missing required columns: {missing}")

    conformed = gold_frame[required].copy()
    conformed["source_name"] = source_name.strip().upper()
    conformed["dataset_id"] = dataset_id.strip()
    conformed["frequency"] = "A"
    conformed["period_code"] = conformed["year"].astype("Int64").astype(str)
    conformed["vintage_tag"] = vintage_tag.strip()
    conformed = conformed[
        [
            "source_name",
            "dataset_id",
            "bea_table_name",
            "frequency",
            "period_code",
            "year",
            "state_fips",
            "state_abbrev",
            "geo_name",
            "line_code",
            "series_code",
            "series_name",
            "function_name",
            "unit",
            "unit_mult",
            "vintage_tag",
            "pce_value",
            "pce_value_scaled",
            "note_ref",
        ]
    ]
    return conformed
