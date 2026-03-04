from __future__ import annotations

from dataclasses import dataclass
import time
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

    def __init__(
        self,
        api_key: str,
        timeout_seconds: int = 60,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        min_request_interval_seconds: float = 0.25,
    ) -> None:
        self.api_key = api_key
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.min_request_interval_seconds = min_request_interval_seconds
        self._session = requests.Session()
        self._last_request_monotonic: float | None = None

    def _throttle_if_needed(self) -> None:
        if self._last_request_monotonic is None:
            return
        elapsed = time.monotonic() - self._last_request_monotonic
        remaining = self.min_request_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)

    def _request(self, params: dict[str, str]) -> dict[str, Any]:
        last_exception: Exception | None = None
        retryable_statuses = {429, 500, 502, 503, 504}
        for attempt in range(self.max_retries + 1):
            self._throttle_if_needed()
            self._last_request_monotonic = time.monotonic()
            try:
                response = self._session.get(
                    self.base_url,
                    params=params,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:
                last_exception = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code not in retryable_statuses or attempt == self.max_retries:
                    raise
                retry_after = None
                if exc.response is not None:
                    retry_after = exc.response.headers.get("Retry-After")
                if retry_after:
                    wait_seconds = float(retry_after)
                else:
                    wait_seconds = self.retry_backoff_seconds * (2**attempt)
                time.sleep(wait_seconds)
            except requests.RequestException as exc:
                last_exception = exc
                if attempt == self.max_retries:
                    raise
                wait_seconds = self.retry_backoff_seconds * (2**attempt)
                time.sleep(wait_seconds)
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Unexpected request retry failure without exception.")

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
