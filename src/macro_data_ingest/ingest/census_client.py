from __future__ import annotations

import re
from typing import Any

import requests

from macro_data_ingest.ingest.http_utils import JsonHttpClient


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
        self._http = JsonHttpClient(
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            min_request_interval_seconds=min_request_interval_seconds,
        )

    def _request(
        self, year: int | None, dataset_path: str, params: dict[str, str]
    ) -> list[list[str]]:
        normalized_path = dataset_path.strip("/")
        if year is None:
            url = f"{self.base_url}/{normalized_path}"
        else:
            url = f"{self.base_url}/{year}/{normalized_path}"
        payload = self._http.request_json(url=url, params=params)
        if not isinstance(payload, list):
            raise ValueError("Unexpected Census response format: expected list payload.")
        return payload

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

    def fetch_state_timeseries_metric(
        self,
        *,
        years: list[int],
        dataset_path: str,
        value_column: str,
        predicates: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        normalized_value_column = value_column.strip().upper()
        extra_predicates = predicates or {}
        for year in years:
            params = {
                "get": f"NAME,{normalized_value_column}",
                "for": "state:*",
                "time": str(year),
                "key": self.api_key,
            }
            params.update(extra_predicates)
            try:
                payload = self._request(
                    year=None,
                    dataset_path=dataset_path,
                    params=params,
                )
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in {400, 404}:
                    continue
                raise
            except ValueError:
                # Some years can return non-JSON/empty payloads even with 200 responses.
                continue
            if len(payload) < 2:
                continue
            header = payload[0]
            header_index = {name: idx for idx, name in enumerate(header)}
            required = ["NAME", normalized_value_column, "state", "time"]
            missing = [name for name in required if name not in header_index]
            if missing:
                raise ValueError(
                    "Census timeseries response missing required columns "
                    f"for year={year}: {missing}"
                )
            for record in payload[1:]:
                time_value = str(record[header_index["time"]]).strip()
                match = re.search(r"(\d{4})", time_value)
                if match is None:
                    continue
                rows.append(
                    {
                        "NAME": record[header_index["NAME"]],
                        normalized_value_column: record[header_index[normalized_value_column]],
                        "state": record[header_index["state"]],
                        "YEAR": match.group(1),
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
