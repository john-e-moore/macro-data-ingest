from macro_data_ingest.transforms.series_labels import hierarchy_path_to_json, parse_bea_series_label


def test_parse_bea_series_label_single_level() -> None:
    series_name, function_name, hierarchy_path = parse_bea_series_label(
        "Personal consumption expenditures"
    )
    assert series_name == ""
    assert function_name == "Personal consumption expenditures"
    assert hierarchy_path == ["Personal consumption expenditures"]


def test_parse_bea_series_label_two_levels_with_table_prefix() -> None:
    series_name, function_name, hierarchy_path = parse_bea_series_label(
        "[SAPCE4] Total personal consumption expenditures: Life insurance"
    )
    assert series_name == "Total personal consumption expenditures"
    assert function_name == "Life insurance"
    assert hierarchy_path == ["Total personal consumption expenditures", "Life insurance"]


def test_parse_bea_series_label_multi_level_hierarchy() -> None:
    series_name, function_name, hierarchy_path = parse_bea_series_label(
        "[SAPCE1] Goods: Durable goods: Motor vehicles and parts"
    )
    assert series_name == "Goods"
    assert function_name == "Motor vehicles and parts"
    assert hierarchy_path == ["Goods", "Durable goods", "Motor vehicles and parts"]


def test_hierarchy_path_to_json_serializes_ascii() -> None:
    assert hierarchy_path_to_json(["Goods", "Durable goods"]) == '["Goods", "Durable goods"]'
