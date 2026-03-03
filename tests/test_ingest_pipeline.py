from datetime import datetime, timezone

import pytest

from macro_data_ingest.ingest.pipeline import _is_changed, _source_release_tag, _year_range
from macro_data_ingest.run_metadata import stable_rows_hash


def test_is_changed_when_no_checkpoint() -> None:
    assert _is_changed(None, "abc")


def test_is_changed_when_hash_matches() -> None:
    checkpoint = {"payload_hash": "abc"}
    assert not _is_changed(checkpoint, "abc")


def test_is_changed_when_hash_differs() -> None:
    checkpoint = {"payload_hash": "abc"}
    assert _is_changed(checkpoint, "def")


def test_year_range_starts_at_configured_year() -> None:
    current_year = datetime.now(timezone.utc).year
    year_range = _year_range(2000)
    assert year_range.startswith("2000,2001")
    assert year_range.endswith(str(current_year))


def test_year_range_rejects_future_year() -> None:
    current_year = datetime.now(timezone.utc).year
    with pytest.raises(ValueError):
        _year_range(current_year + 1)


def test_source_release_tag_prefers_release_date() -> None:
    payload = {"BEAAPI": {"Results": {"ReleaseDate": "2026-03-01", "ReleaseName": "Ignored"}}}
    assert _source_release_tag(payload) == "2026-03-01"


def test_source_release_tag_falls_back_to_none() -> None:
    payload = {"BEAAPI": {"Results": {"Data": []}}}
    assert _source_release_tag(payload) is None


def test_stable_rows_hash_ignores_row_order() -> None:
    rows_a = [
        {"Code": "SAPCE3-1", "GeoFips": "01000", "TimePeriod": "2000", "DataValue": "1"},
        {"Code": "SAPCE3-1", "GeoFips": "02000", "TimePeriod": "2000", "DataValue": "2"},
    ]
    rows_b = [rows_a[1], rows_a[0]]
    assert stable_rows_hash(rows_a) == stable_rows_hash(rows_b)
