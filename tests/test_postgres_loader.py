import pytest

from macro_data_ingest.load.postgres_loader import PostgresLoader


def test_validate_identifier_accepts_safe_names() -> None:
    assert PostgresLoader._validate_identifier("valid_name_1") == "valid_name_1"


def test_validate_identifier_rejects_unsafe_names() -> None:
    with pytest.raises(ValueError):
        PostgresLoader._validate_identifier("bad-name;drop")


def test_annual_period_bounds_uses_calendar_year() -> None:
    start, end, month, quarter = PostgresLoader._period_bounds("A", "2024", 2024, None, None)
    assert start == "2024-01-01"
    assert end == "2024-12-31"
    assert month is None
    assert quarter is None


def test_monthly_period_bounds_uses_calendar_month() -> None:
    start, end, month, quarter = PostgresLoader._period_bounds("M", "2024M02", 2024, 2, None)
    assert start == "2024-02-01"
    assert end == "2024-02-29"
    assert month == 2
    assert quarter is None
