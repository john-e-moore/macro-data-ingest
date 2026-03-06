from __future__ import annotations

import pandas as pd

from macro_data_ingest.datasets import CensusDatasetSpec
from macro_data_ingest.transforms.silver import STATE_FIPS_TO_ABBR


def to_census_silver_frame(raw_payload: dict, dataset_spec: CensusDatasetSpec) -> pd.DataFrame:
    """Convert raw Census payload into typed Silver records for state annual population rows."""
    variable = dataset_spec.census_variable.strip().upper()
    rows = raw_payload.get("CENSUSAPI", {}).get("Results", {}).get("Data", [])
    if not rows:
        return pd.DataFrame(
            columns=[
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
        )

    frame = pd.DataFrame(rows)
    if variable not in frame.columns:
        raise ValueError(f"Census payload missing variable column: {variable}")
    frame["state_fips"] = frame["state"].astype(str).str.zfill(2)
    frame["state_abbrev"] = frame["state_fips"].map(STATE_FIPS_TO_ABBR)
    frame = frame[frame["state_abbrev"].notna()].copy()
    frame["geo_name"] = frame["NAME"].astype(str)
    frame["year"] = pd.to_numeric(frame["YEAR"], errors="coerce").astype("Int64")
    frame = frame[frame["year"].notna()].copy()
    frame["frequency"] = "A"
    frame["period_code"] = frame["year"].astype("Int64").astype(str)
    frame["population"] = pd.to_numeric(frame[variable], errors="coerce")
    frame["census_dataset_path"] = dataset_spec.census_dataset_path.strip().lower()
    frame["census_variable"] = variable
    frame["unit"] = "persons"
    frame["note_ref"] = ""
    silver = frame[
        [
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
    ].sort_values(["period_code", "state_fips"]).reset_index(drop=True)
    return silver


def validate_census_silver_frame(frame: pd.DataFrame) -> None:
    if frame.empty:
        raise ValueError("Census silver frame is empty.")

    required_not_null = [
        "state_fips",
        "state_abbrev",
        "geo_name",
        "frequency",
        "period_code",
        "year",
        "population",
        "census_dataset_path",
        "census_variable",
    ]
    null_counts = frame[required_not_null].isnull().sum()
    bad_cols = [col for col, count in null_counts.items() if count > 0]
    if bad_cols:
        raise ValueError(f"Census silver frame has nulls in required columns: {bad_cols}")

    dupes = frame.duplicated(
        subset=["state_fips", "period_code", "census_variable"]
    ).sum()
    if dupes > 0:
        raise ValueError(f"Census silver frame has duplicate primary keys: {dupes}")
