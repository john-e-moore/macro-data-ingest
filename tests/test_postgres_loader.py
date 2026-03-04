import pytest

from macro_data_ingest.load.postgres_loader import PostgresLoader


def test_validate_identifier_accepts_safe_names() -> None:
    assert PostgresLoader._validate_identifier("valid_name_1") == "valid_name_1"


def test_validate_identifier_rejects_unsafe_names() -> None:
    with pytest.raises(ValueError):
        PostgresLoader._validate_identifier("bad-name;drop")


def test_annual_period_bounds_uses_calendar_year() -> None:
    start, end = PostgresLoader._annual_period_bounds(2024)
    assert start == "2024-01-01"
    assert end == "2024-12-31"
