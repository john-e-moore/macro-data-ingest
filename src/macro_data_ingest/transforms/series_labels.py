from __future__ import annotations

import json
import re


def parse_bea_series_label(raw_label: str) -> tuple[str, str, list[str]]:
    """Parse BEA line description into compatibility fields + hierarchy path.

    Returns `(series_name, function_name, hierarchy_path)`.
    - `series_name`/`function_name` keep backward-compatible semantics.
    - `hierarchy_path` captures all tiers when labels are nested.
    """
    label = str(raw_label or "").strip()
    if not label:
        return "", "", []

    # Remove optional BEA table prefix like "[SAPCE4] ".
    normalized = re.sub(r"^\[[^\]]+\]\s*", "", label).strip()
    if not normalized:
        return "", "", []

    parts = [part.strip() for part in normalized.split(":")]
    hierarchy_path = [part for part in parts if part]
    if not hierarchy_path:
        return "", "", []

    if len(hierarchy_path) == 1:
        return "", hierarchy_path[0], hierarchy_path

    return hierarchy_path[0], hierarchy_path[-1], hierarchy_path


def hierarchy_path_to_json(path: list[str]) -> str:
    return json.dumps(path, ensure_ascii=True)
