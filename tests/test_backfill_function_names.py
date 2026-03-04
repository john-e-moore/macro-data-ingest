from scripts.backfill_function_names import _default_run_id, _parse_table_names


def test_parse_table_names_normalizes_and_splits() -> None:
    assert _parse_table_names("sapce3, SAPCE4 ,sapce4") == ["SAPCE3", "SAPCE4", "SAPCE4"]


def test_parse_table_names_rejects_empty() -> None:
    try:
        _parse_table_names(" , ")
        assert False, "Expected ValueError"
    except ValueError:
        assert True


def test_default_run_id_prefix() -> None:
    run_id = _default_run_id()
    assert run_id.startswith("backfill-function-name-")
