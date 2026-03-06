from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from macro_data_ingest.ingest.http_utils import JsonHttpClient


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

    def __init__(
        self,
        api_key: str,
        timeout_seconds: int = 60,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        min_request_interval_seconds: float = 0.25,
    ) -> None:
        self.api_key = api_key
        self._http = JsonHttpClient(
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            min_request_interval_seconds=min_request_interval_seconds,
        )

    def _request(self, params: dict[str, str]) -> dict[str, Any]:
        payload = self._http.request_json(
            url=self.base_url,
            params=params,
            honor_retry_after_header=True,
        )
        if not isinstance(payload, dict):
            raise ValueError("Unexpected BEA response format: expected object payload.")
        return payload

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
        payload = self._request(self._build_params(query))
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

    def fetch_line_codes(self, dataset: str, table_name: str) -> list[str]:
        return list(self.fetch_line_code_descriptions(dataset, table_name).keys())

    def fetch_line_code_descriptions(self, dataset: str, table_name: str) -> dict[str, str]:
        payload = self._request(
            {
                "UserID": self.api_key,
                "method": "GetParameterValuesFiltered",
                "datasetname": dataset,
                "TargetParameter": "LineCode",
                "TableName": table_name,
                "ResultFormat": "JSON",
            }
        )
        error = payload.get("BEAAPI", {}).get("Error")
        if error:
            description = error.get("APIErrorDescription", "Unknown BEA API error")
            detail = error.get("ErrorDetail", {}).get("Description", "")
            raise ValueError(f"BEA API error: {description}. {detail}".strip())
        values = payload.get("BEAAPI", {}).get("Results", {}).get("ParamValue", [])
        mapping = {
            str(item.get("Key", "")).strip(): str(item.get("Desc", "")).strip()
            for item in values
            if str(item.get("Key", "")).strip()
        }
        if not mapping:
            raise ValueError(
                f"No LineCode values returned for dataset={dataset} table={table_name}."
            )
        return mapping
