from __future__ import annotations


def build_serving_view_sql(schema_gold: str, schema_serving: str) -> str:
    return f"""
    DROP VIEW IF EXISTS {schema_serving}.v_state_rpp_pce_weighted_annual;
    DROP VIEW IF EXISTS {schema_serving}.v_pce_state_per_capita_annual;
    DROP VIEW IF EXISTS {schema_serving}.v_state_federal_to_stategov_gdp_annual;
    DROP VIEW IF EXISTS {schema_serving}.v_state_federal_to_persons_gdp_annual;
    DROP VIEW IF EXISTS {schema_serving}.v_macro_yoy;
    DROP VIEW IF EXISTS {schema_serving}.obt_state_macro_annual_latest;

    CREATE VIEW {schema_serving}.obt_state_macro_annual_latest AS
    SELECT
        src.source_name,
        src.dataset_id,
        src.table_name AS bea_table_name,
        g.state_fips,
        g.state_abbrev,
        g.geo_name,
        p.year,
        s.line_code,
        s.series_code,
        s.series_name,
        s.function_name,
        s.raw_description,
        s.hierarchy_path_json,
        s.unit,
        s.unit_mult,
        v.vintage_tag,
        v.release_tag,
        f.value_numeric AS pce_value,
        f.value_scaled AS pce_value_scaled,
        f.note_ref
    FROM {schema_gold}.fact_macro_observation f
    JOIN {schema_gold}.dim_source src
        ON f.source_id = src.source_id
    JOIN {schema_gold}.dim_geo g
        ON f.geo_id = g.geo_id
    JOIN {schema_gold}.dim_period p
        ON f.period_id = p.period_id
    JOIN {schema_gold}.dim_series s
        ON f.series_id = s.series_id
    JOIN {schema_gold}.dim_vintage v
        ON f.vintage_id = v.vintage_id
    WHERE v.is_latest = TRUE
      AND p.frequency = 'A';

    CREATE VIEW {schema_serving}.v_state_rpp_pce_weighted_annual AS
    WITH rpp_rows AS (
        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            line_code AS rpp_line_code,
            series_code AS rpp_series_code,
            function_name AS rpp_function_name,
            pce_value AS rpp,
            vintage_tag AS rpp_vintage_tag,
            release_tag AS rpp_release_tag,
            CASE line_code
                WHEN '1' THEN 'all_items'
                WHEN '2' THEN 'goods'
                WHEN '3' THEN 'housing_rents'
                WHEN '4' THEN 'utilities'
                WHEN '5' THEN 'other_services'
                ELSE NULL
            END AS category_key,
            CASE line_code
                WHEN '1' THEN 'All items'
                WHEN '2' THEN 'Goods'
                WHEN '3' THEN 'Housing rents'
                WHEN '4' THEN 'Utilities'
                WHEN '5' THEN 'Other services'
                ELSE function_name
            END AS category
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SARPP'
    ),
    pce_rows AS (
        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            'all_items' AS category_key,
            'All items' AS category,
            'direct' AS mapping_method,
            'SAPCE1' AS pce_source_table,
            series_code AS pce_series_code,
            function_name AS pce_function_name,
            pce_value_scaled AS pce,
            vintage_tag AS pce_vintage_tag,
            release_tag AS pce_release_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAPCE1'
          AND line_code = '1'

        UNION ALL

        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            'goods' AS category_key,
            'Goods' AS category,
            'direct' AS mapping_method,
            'SAPCE1' AS pce_source_table,
            series_code AS pce_series_code,
            function_name AS pce_function_name,
            pce_value_scaled AS pce,
            vintage_tag AS pce_vintage_tag,
            release_tag AS pce_release_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAPCE1'
          AND line_code = '2'

        UNION ALL

        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            'housing_rents' AS category_key,
            'Housing rents' AS category,
            'approximation_housing_proxy_for_rents' AS mapping_method,
            'SAPCE4' AS pce_source_table,
            series_code AS pce_series_code,
            function_name AS pce_function_name,
            pce_value_scaled AS pce,
            vintage_tag AS pce_vintage_tag,
            release_tag AS pce_release_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAPCE4'
          AND line_code = '19'

        UNION ALL

        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            'utilities' AS category_key,
            'Utilities' AS category,
            'direct' AS mapping_method,
            'SAPCE4' AS pce_source_table,
            series_code AS pce_series_code,
            function_name AS pce_function_name,
            pce_value_scaled AS pce,
            vintage_tag AS pce_vintage_tag,
            release_tag AS pce_release_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAPCE4'
          AND line_code = '24'

        UNION ALL

        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            'other_services' AS category_key,
            'Other services' AS category,
            'derived_services_less_housing_and_utilities' AS mapping_method,
            'SAPCE1' AS pce_source_table,
            'SAPCE1-13_MINUS_15' AS pce_series_code,
            'Services less housing and utilities' AS pce_function_name,
            MAX(CASE WHEN line_code = '13' THEN pce_value_scaled END)
                - MAX(CASE WHEN line_code = '15' THEN pce_value_scaled END) AS pce,
            MAX(vintage_tag) AS pce_vintage_tag,
            MAX(release_tag) AS pce_release_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAPCE1'
          AND line_code IN ('13', '15')
        GROUP BY
            state_fips,
            state_abbrev,
            geo_name,
            year
    ),
    national_pce_totals AS (
        SELECT
            year,
            category_key,
            SUM(pce) AS national_pce
        FROM pce_rows
        GROUP BY year, category_key
    ),
    weighted_rows AS (
        SELECT
            r.year,
            r.state_fips,
            r.state_abbrev,
            r.geo_name,
            r.category,
            r.rpp_line_code,
            r.rpp_series_code,
            r.rpp_function_name,
            r.rpp,
            p.pce_source_table,
            p.pce_series_code,
            p.pce_function_name,
            p.mapping_method,
            p.pce,
            CASE
                WHEN p.pce IS NULL THEN NULL
                ELSE p.pce / NULLIF(t.national_pce, 0)
            END AS pce_share,
            r.rpp_vintage_tag,
            r.rpp_release_tag,
            p.pce_vintage_tag,
            p.pce_release_tag
        FROM rpp_rows r
        JOIN pce_rows p
            ON r.state_fips = p.state_fips
           AND r.year = p.year
           AND r.category_key = p.category_key
        JOIN national_pce_totals t
            ON p.year = t.year
           AND p.category_key = t.category_key
        WHERE r.category_key IS NOT NULL
    )
    SELECT
        year,
        state_fips,
        state_abbrev,
        geo_name,
        category,
        rpp_line_code,
        rpp_series_code,
        rpp_function_name,
        rpp,
        pce_source_table,
        pce_series_code,
        pce_function_name,
        mapping_method,
        pce,
        pce_share,
        CASE
            WHEN rpp IS NULL OR pce_share IS NULL THEN NULL
            ELSE rpp * pce_share
        END AS weighted_rpp,
        rpp_vintage_tag,
        rpp_release_tag,
        pce_vintage_tag,
        pce_release_tag
    FROM weighted_rows;

    CREATE VIEW {schema_serving}.v_macro_yoy AS
    SELECT
        cur.source_name,
        cur.dataset_id,
        cur.bea_table_name,
        cur.state_fips,
        cur.state_abbrev,
        cur.geo_name,
        cur.line_code,
        cur.series_code,
        cur.series_name,
        cur.function_name,
        cur.raw_description,
        cur.hierarchy_path_json,
        cur.year,
        cur.pce_value AS value_current,
        prev.pce_value AS value_prior,
        CASE
            WHEN prev.pce_value IS NULL OR prev.pce_value = 0 THEN NULL
            ELSE ((cur.pce_value - prev.pce_value) / prev.pce_value) * 100.0
        END AS yoy_pct
    FROM {schema_serving}.obt_state_macro_annual_latest cur
    LEFT JOIN {schema_serving}.obt_state_macro_annual_latest prev
        ON cur.source_name = prev.source_name
       AND cur.dataset_id = prev.dataset_id
       AND cur.state_fips = prev.state_fips
       AND cur.series_code = prev.series_code
       AND cur.year = prev.year + 1;

    CREATE VIEW {schema_serving}.v_pce_state_per_capita_annual AS
    WITH bea_rows AS (
        SELECT *
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
    ),
    population_rows AS (
        SELECT
            state_fips,
            year,
            pce_value_scaled AS population,
            vintage_tag AS population_vintage_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'CENSUS'
    )
    SELECT
        b.bea_table_name,
        b.state_fips,
        b.state_abbrev,
        b.geo_name,
        b.year,
        b.line_code,
        b.series_code,
        b.series_name,
        b.function_name,
        b.unit AS bea_unit,
        b.pce_value,
        b.pce_value_scaled,
        p.population,
        p.population_vintage_tag,
        CASE
            WHEN p.population IS NULL OR p.population = 0 THEN NULL
            ELSE b.pce_value_scaled / p.population
        END AS value_per_capita
    FROM bea_rows b
    LEFT JOIN population_rows p
        ON b.state_fips = p.state_fips
       AND b.year = p.year;

    CREATE VIEW {schema_serving}.v_state_federal_to_stategov_gdp_annual AS
    WITH gdp_rows AS (
        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            pce_value_scaled AS gdp_current_dollars,
            vintage_tag AS gdp_vintage_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAGDP1'
          AND line_code = '3'
    ),
    federal_stategov_rows AS (
        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            pce_value_scaled AS federal_stategov_receipts_dollars,
            vintage_tag AS federal_stategov_vintage_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'CENSUS'
          AND dataset_id = 'census_state_gov_finance_federal_intergovernmental_revenue'
    )
    SELECT
        n.state_fips,
        n.state_abbrev,
        n.geo_name,
        n.year,
        n.federal_stategov_receipts_dollars,
        d.gdp_current_dollars,
        CASE
            WHEN d.gdp_current_dollars IS NULL OR d.gdp_current_dollars = 0 THEN NULL
            ELSE n.federal_stategov_receipts_dollars / d.gdp_current_dollars
        END AS federal_stategov_to_gdp_ratio,
        n.federal_stategov_vintage_tag,
        d.gdp_vintage_tag
    FROM federal_stategov_rows n
    LEFT JOIN gdp_rows d
        ON n.state_fips = d.state_fips
       AND n.year = d.year;

    CREATE VIEW {schema_serving}.v_state_federal_to_persons_gdp_annual AS
    WITH gdp_rows AS (
        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            pce_value_scaled AS gdp_current_dollars,
            vintage_tag AS gdp_vintage_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND bea_table_name = 'SAGDP1'
          AND line_code = '3'
    ),
    federal_persons_rows AS (
        SELECT
            state_fips,
            state_abbrev,
            geo_name,
            year,
            pce_value_scaled AS federal_to_persons_receipts_dollars,
            vintage_tag AS federal_to_persons_vintage_tag
        FROM {schema_serving}.obt_state_macro_annual_latest
        WHERE source_name = 'BEA'
          AND dataset_id = 'state_personal_transfer_receipts_sainc35'
          AND line_code = '2000'
    )
    SELECT
        n.state_fips,
        n.state_abbrev,
        n.geo_name,
        n.year,
        n.federal_to_persons_receipts_dollars,
        d.gdp_current_dollars,
        CASE
            WHEN d.gdp_current_dollars IS NULL OR d.gdp_current_dollars = 0 THEN NULL
            ELSE n.federal_to_persons_receipts_dollars / d.gdp_current_dollars
        END AS federal_to_persons_to_gdp_ratio,
        n.federal_to_persons_vintage_tag,
        d.gdp_vintage_tag
    FROM federal_persons_rows n
    LEFT JOIN gdp_rows d
        ON n.state_fips = d.state_fips
       AND n.year = d.year;
    """
