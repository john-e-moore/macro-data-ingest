from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class BeaQuery:
    dataset: str
    table_name: str
    frequency: str = "A"
    year: str = "ALL"
    geo_fips: str = "STATE"
    line_code: str = "ALL"


class BeaClient:
    """BEA API client for GetData calls."""

    base_url = "https://apps.bea.gov/api/data"

    def __init__(self, api_key: str, timeout_seconds: int = 60) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self._session = requests.Session()

    def _build_params(self, query: BeaQuery) -> dict[str, str]:
        params = {
            "UserID": self.api_key,
            "method": "GetData",
            "datasetname": query.dataset,
            "TableName": query.table_name,
            "Frequency": query.frequency,
            "Year": query.year,
            "GeoFips": query.geo_fips,
            "ResultFormat": "JSON",
        }
        # Some BEA tables support all rows when LineCode is omitted.
        if query.line_code.upper() != "ALL":
            params["LineCode"] = query.line_code
        return params

    def fetch(self, query: BeaQuery) -> dict[str, Any]:
        response = self._session.get(
            self.base_url,
            params=self._build_params(query),
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        if "BEAAPI" not in payload:
            raise ValueError("Unexpected BEA response format: missing BEAAPI.")
        error = payload.get("BEAAPI", {}).get("Error")
        if error:
            description = error.get("APIErrorDescription", "Unknown BEA API error")
            detail = error.get("ErrorDetail", {}).get("Description", "")
            raise ValueError(f"BEA API error: {description}. {detail}".strip())
        return payload

    @staticmethod
    def extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])
