from macro_data_ingest.load.serving_views import build_serving_view_sql


def test_build_serving_view_sql_includes_weighted_rpp_view() -> None:
    sql = build_serving_view_sql(schema_gold="gold", schema_serving="serving")

    assert "DROP VIEW IF EXISTS serving.v_state_rpp_pce_weighted_annual;" in sql
    assert "CREATE VIEW serving.v_state_rpp_pce_weighted_annual AS" in sql
    assert "AND bea_table_name = 'SARPP'" in sql
    assert "AND bea_table_name = 'SAPCE1'" in sql
    assert "AND bea_table_name = 'SAPCE4'" in sql
    assert "WHEN '3' THEN 'housing_rents'" in sql
    assert "WHEN '4' THEN 'utilities'" in sql
    assert "WHEN '5' THEN 'other_services'" in sql
    assert "AND line_code = '19'" in sql
    assert "AND line_code = '24'" in sql
    assert "AND line_code IN ('13', '15')" in sql
    assert "MAX(CASE WHEN line_code = '13' THEN pce_value_scaled END)" in sql
    assert "MAX(CASE WHEN line_code = '15' THEN pce_value_scaled END)" in sql
    assert "SUM(pce) AS national_pce" in sql
    assert "ELSE p.pce / NULLIF(t.national_pce, 0)" in sql
    assert "ELSE rpp * pce_share" in sql


def test_build_serving_view_sql_exposes_weighted_rpp_columns() -> None:
    sql = build_serving_view_sql(schema_gold="gold", schema_serving="serving")

    assert "rpp_line_code" in sql
    assert "rpp_series_code" in sql
    assert "pce_source_table" in sql
    assert "pce_series_code" in sql
    assert "mapping_method" in sql
    assert "pce_share" in sql
    assert "weighted_rpp" in sql
