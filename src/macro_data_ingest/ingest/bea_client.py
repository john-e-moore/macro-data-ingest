from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class BeaQuery:
    dataset: str
    table_name: str
    frequency: str = "A"
    year: str = "ALL"


class BeaClient:
    """Scaffold for BEA API access.

    Real request/response behavior will be implemented in a vertical slice.
    """

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key

    def fetch(self, query: BeaQuery) -> dict[str, Any]:
        raise NotImplementedError("BEA fetch is not implemented yet.")
