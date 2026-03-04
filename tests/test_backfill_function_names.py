from scripts.backfill_function_names import _default_run_id, _parse_table_names, _split_function_label


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


def test_split_function_label_removes_table_prefix() -> None:
    series_name, function_name = _split_function_label(
        "[SAPCE4] Total personal consumption expenditures: Life insurance"
    )
    assert series_name == "Total personal consumption expenditures"
    assert function_name == "Life insurance"
