from __future__ import annotations

import pandas as pd

STATE_FIPS_TO_ABBR = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
}


def _parse_state_fips(geo_fips: str) -> str:
    if len(geo_fips) == 5 and geo_fips.endswith("000"):
        return geo_fips[:2]
    return geo_fips[:2]


def _parse_line_code(series_code: str) -> str:
    if "-" in series_code:
        return series_code.split("-", maxsplit=1)[1]
    return series_code


def to_silver_frame(raw_payload: dict) -> pd.DataFrame:
    """Convert raw BEA payload into typed Silver records for state-level rows."""
    rows = raw_payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
    if not rows:
        return pd.DataFrame(
            columns=[
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
        )

    frame = pd.DataFrame(rows)
    frame["state_fips"] = frame["GeoFips"].astype(str).map(_parse_state_fips)
    frame["state_abbrev"] = frame["state_fips"].map(STATE_FIPS_TO_ABBR)
    # Keep only state + DC rows for this slice.
    frame = frame[frame["state_abbrev"].notna()].copy()

    frame["year"] = pd.to_numeric(frame["TimePeriod"], errors="coerce").astype("Int64")
    frame["line_code"] = frame["Code"].astype(str).map(_parse_line_code)
    frame["value"] = pd.to_numeric(frame["DataValue"].astype(str).str.replace(",", ""), errors="coerce")
    frame["unit_mult"] = pd.to_numeric(frame["UNIT_MULT"], errors="coerce").fillna(0).astype(int)

    silver = frame[
        [
            "state_fips",
            "state_abbrev",
            "GeoName",
            "year",
            "line_code",
            "Code",
            "value",
            "CL_UNIT",
            "unit_mult",
            "NoteRef",
        ]
    ].rename(
        columns={
            "GeoName": "geo_name",
            "Code": "series_code",
            "CL_UNIT": "unit",
            "NoteRef": "note_ref",
        }
    )
    silver = silver.sort_values(["year", "state_fips", "line_code"]).reset_index(drop=True)
    return silver


def validate_silver_frame(frame: pd.DataFrame) -> None:
    if frame.empty:
        raise ValueError("Silver frame is empty.")

    required_not_null = ["state_fips", "state_abbrev", "year", "line_code", "value"]
    null_counts = frame[required_not_null].isnull().sum()
    bad_cols = [col for col, count in null_counts.items() if count > 0]
    if bad_cols:
        raise ValueError(f"Silver frame has nulls in required columns: {bad_cols}")

    dupes = frame.duplicated(subset=["state_fips", "year", "line_code"]).sum()
    if dupes > 0:
        raise ValueError(f"Silver frame has duplicate primary keys: {dupes}")
