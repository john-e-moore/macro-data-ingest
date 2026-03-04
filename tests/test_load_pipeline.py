import pytest

from macro_data_ingest.load.pipeline import _extract_vintage_tag_from_silver_key


def test_extract_vintage_tag_from_silver_key() -> None:
    key = "staging/silver/bea/pce_state_sapce4/extract_date=2026-03-04/run_id=run-1/part-000.parquet"
    assert _extract_vintage_tag_from_silver_key(key) == "2026-03-04"


def test_extract_vintage_tag_from_silver_key_requires_partition() -> None:
    with pytest.raises(ValueError):
        _extract_vintage_tag_from_silver_key("staging/silver/bea/pce_state_sapce4/part-000.parquet")
