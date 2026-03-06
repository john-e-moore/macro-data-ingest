from __future__ import annotations

import time
from typing import Any

import requests


class JsonHttpClient:
    """Shared JSON HTTP helper with throttling and retry logic."""

    def __init__(
        self,
        *,
        timeout_seconds: int = 60,
        max_retries: int = 5,
        retry_backoff_seconds: float = 1.0,
        min_request_interval_seconds: float = 0.25,
    ) -> None:
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

    def request_json(
        self,
        *,
        url: str,
        params: dict[str, str],
        retryable_statuses: set[int] | None = None,
        honor_retry_after_header: bool = False,
    ) -> Any:
        last_exception: Exception | None = None
        allowed_statuses = retryable_statuses or {429, 500, 502, 503, 504}
        for attempt in range(self.max_retries + 1):
            self._throttle_if_needed()
            self._last_request_monotonic = time.monotonic()
            try:
                response = self._session.get(url, params=params, timeout=self.timeout_seconds)
                response.raise_for_status()
                if not response.text.strip():
                    return []
                return response.json()
            except requests.HTTPError as exc:
                last_exception = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code not in allowed_statuses or attempt == self.max_retries:
                    raise
                if honor_retry_after_header and exc.response is not None:
                    retry_after = exc.response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            time.sleep(float(retry_after))
                            continue
                        except ValueError:
                            pass
                time.sleep(self.retry_backoff_seconds * (2**attempt))
            except requests.RequestException as exc:
                last_exception = exc
                if attempt == self.max_retries:
                    raise
                time.sleep(self.retry_backoff_seconds * (2**attempt))
        if last_exception is not None:
            raise last_exception
        raise RuntimeError("Unexpected request retry failure without exception.")
