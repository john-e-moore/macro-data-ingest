from __future__ import annotations

import re
import time
from typing import Any

import requests


class CensusClient:
    """Census Data API client for simple tabular pulls."""

    base_url = "https://api.census.gov/data"

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

    def _request(self, year: int, dataset_path: str, params: dict[str, str]) -> list[list[str]]:
        last_exception: Exception | None = None
        retryable_statuses = {429, 500, 502, 503, 504}
        url = f"{self.base_url}/{year}/{dataset_path.strip('/')}"
        for attempt in range(self.max_retries + 1):
            self._throttle_if_needed()
            self._last_request_monotonic = time.monotonic()
            try:
                response = self._session.get(url, params=params, timeout=self.timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, list):
                    raise ValueError("Unexpected Census response format: expected list payload.")
                return payload
            except requests.HTTPError as exc:
                last_exception = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code not in retryable_statuses or attempt == self.max_retries:
                    raise
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

    def fetch_state_population(
        self,
        *,
        years: list[int],
        dataset_path: str,
        variable: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        variable_name = variable.strip().upper()
        for year in years:
            try:
                payload = self._request(
                    year=year,
                    dataset_path=dataset_path,
                    params={
                        "get": f"NAME,{variable_name}",
                        "for": "state:*",
                        "key": self.api_key,
                    },
                )
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                # Census may not have published the current year endpoint yet.
                if status_code in {400, 404}:
                    continue
                raise
            if len(payload) < 2:
                continue
            header = payload[0]
            header_index = {name: idx for idx, name in enumerate(header)}
            required = ["NAME", variable_name, "state"]
            missing = [name for name in required if name not in header_index]
            if missing:
                raise ValueError(
                    f"Census response missing required columns for year={year}: {missing}"
                )
            for record in payload[1:]:
                rows.append(
                    {
                        "NAME": record[header_index["NAME"]],
                        variable_name: record[header_index[variable_name]],
                        "state": record[header_index["state"]],
                        "YEAR": str(year),
                    }
                )
        return rows

    @staticmethod
    def _year_from_intercensal_date_desc(date_desc: str) -> int | None:
        match = re.search(r"(\d{4})\s+population", date_desc.lower())
        if match is None:
            return None
        return int(match.group(1))

    def fetch_state_population_intercensal(
        self,
        *,
        years: list[int],
        variable_alias: str,
    ) -> list[dict[str, Any]]:
        if not years:
            return []
        requested_years = {int(year) for year in years}
        payload = self._request(
            year=2000,
            dataset_path="pep/int_population",
            params={
                "get": "GEONAME,POP,DATE_DESC",
                "for": "state:*",
                "key": self.api_key,
            },
        )
        if len(payload) < 2:
            return []
        header = payload[0]
        header_index = {name: idx for idx, name in enumerate(header)}
        required = ["GEONAME", "POP", "DATE_DESC", "state"]
        missing = [name for name in required if name not in header_index]
        if missing:
            raise ValueError(f"Census intercensal response missing required columns: {missing}")

        rows: list[dict[str, Any]] = []
        for record in payload[1:]:
            date_desc = str(record[header_index["DATE_DESC"]]).strip()
            # Keep only July 1 annual estimates and drop April 1 base/census rows.
            if not date_desc.lower().startswith("7/1/"):
                continue
            year_value = self._year_from_intercensal_date_desc(date_desc)
            if year_value is None or year_value not in requested_years:
                continue
            rows.append(
                {
                    "NAME": record[header_index["GEONAME"]],
                    variable_alias: record[header_index["POP"]],
                    "state": record[header_index["state"]],
                    "YEAR": str(year_value),
                }
            )
        return rows
