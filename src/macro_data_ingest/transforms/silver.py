from __future__ import annotations

import re

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


def _parse_time_period(time_period: str) -> tuple[str, int | None, int | None, int | None]:
    value = str(time_period).strip().upper()
    if re.fullmatch(r"\d{4}", value):
        return "A", int(value), None, None
    monthly_match = re.fullmatch(r"(\d{4})M(0[1-9]|1[0-2])", value)
    if monthly_match:
        return "M", int(monthly_match.group(1)), int(monthly_match.group(2)), None
    quarterly_match = re.fullmatch(r"(\d{4})Q([1-4])", value)
    if quarterly_match:
        return "Q", int(quarterly_match.group(1)), None, int(quarterly_match.group(2))
    return "", None, None, None


def _split_function_name(raw_function_name: str) -> tuple[str, str]:
    label = str(raw_function_name or "").strip()
    if not label:
        return "", ""

    if ":" not in label:
        return "", label

    left, right = label.split(":", maxsplit=1)
    series_name = re.sub(r"^\[[^\]]+\]\s*", "", left).strip()
    function_name = right.strip()
    return series_name, function_name


def to_silver_frame(
    raw_payload: dict,
    bea_table_name: str | None = None,
    bea_frequency: str | None = None,
) -> pd.DataFrame:
    """Convert raw BEA payload into typed Silver records for state-level rows."""
    rows = raw_payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
    if not rows:
        return pd.DataFrame(
            columns=[
                "state_fips",
                "state_abbrev",
                "geo_name",
                "frequency",
                "period_code",
                "year",
                "month",
                "quarter",
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
        )

    frame = pd.DataFrame(rows)
    frame["state_fips"] = frame["GeoFips"].astype(str).map(_parse_state_fips)
    frame["state_abbrev"] = frame["state_fips"].map(STATE_FIPS_TO_ABBR)
    # Keep only state + DC rows for this slice.
    frame = frame[frame["state_abbrev"].notna()].copy()

    period_parts = frame["TimePeriod"].astype(str).map(_parse_time_period)
    frame["frequency"] = period_parts.str[0]
    frame["year"] = pd.to_numeric(period_parts.str[1], errors="coerce").astype("Int64")
    frame["month"] = pd.to_numeric(period_parts.str[2], errors="coerce").astype("Int64")
    frame["quarter"] = pd.to_numeric(period_parts.str[3], errors="coerce").astype("Int64")
    frame = frame[frame["frequency"].isin(["A", "M"]) & frame["year"].notna()].copy()
    requested_frequency = str(bea_frequency or "").strip().upper()
    if requested_frequency in {"A", "M"}:
        frame = frame[frame["frequency"] == requested_frequency].copy()
    frame["period_code"] = frame["year"].astype("Int64").astype(str)
    monthly_mask = frame["frequency"] == "M"
    frame.loc[monthly_mask, "period_code"] = (
        frame.loc[monthly_mask, "year"].astype("Int64").astype(str)
        + "M"
        + frame.loc[monthly_mask, "month"].astype("Int64").astype(str).str.zfill(2)
    )
    frame["line_code"] = frame["Code"].astype(str).map(_parse_line_code)
    if bea_table_name is not None:
        frame["bea_table_name"] = str(bea_table_name).strip().upper()
    else:
        frame["bea_table_name"] = frame["Code"].astype(str).str.split("-", n=1).str[0].str.upper()
    frame["value"] = pd.to_numeric(frame["DataValue"].astype(str).str.replace(",", ""), errors="coerce")
    frame["unit_mult"] = pd.to_numeric(frame["UNIT_MULT"], errors="coerce").fillna(0).astype(int)
    if "FunctionName" not in frame.columns and "LineDescription" in frame.columns:
        frame["FunctionName"] = frame["LineDescription"]
    if "FunctionName" not in frame.columns:
        frame["FunctionName"] = ""
    function_parts = frame["FunctionName"].fillna("").astype(str).map(_split_function_name)
    frame["SeriesName"] = function_parts.str[0]
    frame["FunctionName"] = function_parts.str[1]

    silver = frame[
        [
            "state_fips",
            "state_abbrev",
            "GeoName",
            "frequency",
            "period_code",
            "year",
            "month",
            "quarter",
            "bea_table_name",
            "line_code",
            "Code",
            "SeriesName",
            "FunctionName",
            "value",
            "CL_UNIT",
            "unit_mult",
            "NoteRef",
        ]
    ].rename(
        columns={
            "GeoName": "geo_name",
            "Code": "series_code",
            "SeriesName": "series_name",
            "FunctionName": "function_name",
            "CL_UNIT": "unit",
            "NoteRef": "note_ref",
        }
    )
    silver["series_name"] = silver["series_name"].fillna("").astype(str)
    silver["function_name"] = silver["function_name"].fillna("").astype(str)
    silver = silver.sort_values(
        ["bea_table_name", "period_code", "state_fips", "line_code"]
    ).reset_index(drop=True)
    return silver


def validate_silver_frame(frame: pd.DataFrame) -> None:
    if frame.empty:
        raise ValueError("Silver frame is empty.")

    required_not_null = [
        "state_fips",
        "state_abbrev",
        "frequency",
        "period_code",
        "year",
        "bea_table_name",
        "line_code",
        "value",
    ]
    null_counts = frame[required_not_null].isnull().sum()
    bad_cols = [col for col, count in null_counts.items() if count > 0]
    if bad_cols:
        raise ValueError(f"Silver frame has nulls in required columns: {bad_cols}")

    dupes = frame.duplicated(
        subset=["bea_table_name", "state_fips", "period_code", "line_code"]
    ).sum()
    if dupes > 0:
        raise ValueError(f"Silver frame has duplicate primary keys: {dupes}")
