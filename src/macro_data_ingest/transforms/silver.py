from __future__ import annotations

import pandas as pd


def to_silver_frame(raw_payload: dict) -> pd.DataFrame:
    """Convert raw BEA payload into a typed Silver DataFrame."""
    raise NotImplementedError("Silver transform is not implemented yet.")
