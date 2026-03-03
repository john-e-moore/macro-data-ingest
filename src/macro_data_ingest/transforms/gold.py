from __future__ import annotations

import pandas as pd


def to_gold_frame(silver_frame: pd.DataFrame) -> pd.DataFrame:
    """Produce analytics-ready Gold frame from Silver records."""
    required = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "year",
        "line_code",
        "series_code",
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
    gold = gold.sort_values(["year", "state_fips", "line_code"]).reset_index(drop=True)
    return gold
