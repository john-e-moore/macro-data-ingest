from __future__ import annotations

import calendar
import json
import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text


class PostgresLoader:
    """Idempotent Postgres upsert loader and serving view manager."""

    def __init__(self, dsn: str, schema_gold: str, schema_meta: str) -> None:
        self.dsn = dsn
        self.schema_gold = schema_gold
        self.schema_meta = schema_meta
        self.schema_serving = "serving"
        self.engine = create_engine(dsn, connect_args={"connect_timeout": 10})

    @staticmethod
    def _validate_identifier(name: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Invalid SQL identifier: {name}")
        return name

    def ensure_base_objects(self) -> None:
        schema_gold = self._validate_identifier(self.schema_gold)
        schema_meta = self._validate_identifier(self.schema_meta)
        schema_serving = self._validate_identifier(self.schema_serving)

        ddl = f"""
        CREATE SCHEMA IF NOT EXISTS {schema_meta};
        CREATE SCHEMA IF NOT EXISTS {schema_gold};
        CREATE SCHEMA IF NOT EXISTS {schema_serving};

        CREATE TABLE IF NOT EXISTS {schema_meta}.ingest_runs (
            run_id TEXT PRIMARY KEY,
            stage TEXT NOT NULL,
            status TEXT NOT NULL,
            details JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.dim_source (
            source_id BIGSERIAL PRIMARY KEY,
            source_name TEXT NOT NULL,
            dataset_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_name, dataset_id, table_name)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.dim_geo (
            geo_id BIGSERIAL PRIMARY KEY,
            geo_level TEXT NOT NULL,
            country_code TEXT NOT NULL,
            state_fips TEXT,
            state_abbrev TEXT,
            geo_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (geo_level, country_code, state_fips)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.dim_period (
            period_id BIGSERIAL PRIMARY KEY,
            frequency TEXT NOT NULL,
            period_code TEXT NOT NULL,
            period_start_date DATE NOT NULL,
            period_end_date DATE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER,
            month INTEGER,
            is_period_end BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (frequency, period_code)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.dim_series (
            series_id BIGSERIAL PRIMARY KEY,
            source_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_source(source_id),
            bea_table_name TEXT NOT NULL,
            line_code TEXT NOT NULL,
            series_code TEXT NOT NULL,
            series_name TEXT NOT NULL,
            function_name TEXT NOT NULL,
            raw_description TEXT NOT NULL DEFAULT '',
            hierarchy_path_json TEXT NOT NULL DEFAULT '[]',
            parse_strategy TEXT NOT NULL DEFAULT 'colon_path',
            unit TEXT NOT NULL,
            unit_mult INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_id, series_code)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.dim_series_node (
            node_id BIGSERIAL PRIMARY KEY,
            source_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_source(source_id),
            bea_table_name TEXT NOT NULL,
            node_key TEXT NOT NULL,
            node_label TEXT NOT NULL,
            node_level INTEGER NOT NULL,
            parent_node_id BIGINT NULL REFERENCES {schema_gold}.dim_series_node(node_id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_id, bea_table_name, node_key)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.bridge_series_node (
            series_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_series(series_id),
            node_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_series_node(node_id),
            path_ordinal INTEGER NOT NULL,
            is_leaf BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (series_id, node_id)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.dim_vintage (
            vintage_id BIGSERIAL PRIMARY KEY,
            source_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_source(source_id),
            vintage_tag TEXT NOT NULL,
            release_tag TEXT,
            as_of_timestamp TIMESTAMPTZ NOT NULL,
            is_latest BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_id, vintage_tag)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.fact_macro_observation (
            source_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_source(source_id),
            series_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_series(series_id),
            geo_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_geo(geo_id),
            period_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_period(period_id),
            vintage_id BIGINT NOT NULL REFERENCES {schema_gold}.dim_vintage(vintage_id),
            value_numeric DOUBLE PRECISION NOT NULL,
            value_scaled DOUBLE PRECISION NOT NULL,
            note_ref TEXT NULL,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source_id, series_id, geo_id, period_id, vintage_id)
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))
            conn.execute(
                text(
                    f"""
                    ALTER TABLE {schema_gold}.dim_series
                        ADD COLUMN IF NOT EXISTS raw_description TEXT NOT NULL DEFAULT '',
                        ADD COLUMN IF NOT EXISTS hierarchy_path_json TEXT NOT NULL DEFAULT '[]',
                        ADD COLUMN IF NOT EXISTS parse_strategy TEXT NOT NULL DEFAULT 'colon_path';
                    DROP VIEW IF EXISTS {schema_serving}.v_pce_state_yoy;
                    DROP TABLE IF EXISTS {schema_gold}.pce_state_monthly;
                    DROP TABLE IF EXISTS {schema_gold}.pce_state_annual;
                    DROP TABLE IF EXISTS {schema_gold}.population_state_annual;
                    DROP TABLE IF EXISTS {schema_gold}.state_gov_finance_annual;
                    """
                )
            )

    @staticmethod
    def _period_bounds(
        frequency: str,
        period_code: str,
        year: int,
        month: int | None,
        quarter: int | None,
    ) -> tuple[str, str, int | None, int | None]:
        normalized_frequency = frequency.strip().upper()
        if normalized_frequency == "A":
            return f"{year:04d}-01-01", f"{year:04d}-12-31", None, None
        if normalized_frequency == "M":
            month_value = month
            if month_value is None:
                match = re.fullmatch(r"\d{4}M(0[1-9]|1[0-2])", period_code.strip().upper())
                if not match:
                    raise ValueError(
                        f"Invalid monthly period_code={period_code}; expected YYYYMmm format."
                    )
                month_value = int(match.group(1))
            period_start = f"{year:04d}-{month_value:02d}-01"
            day_count = calendar.monthrange(year, month_value)[1]
            period_end = f"{year:04d}-{month_value:02d}-{day_count:02d}"
            return period_start, period_end, month_value, None
        raise ValueError(f"Unsupported frequency for conformed load: {frequency}")

    @staticmethod
    def _chunked(records: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
        if size <= 0:
            raise ValueError("Chunk size must be positive.")
        return [records[idx : idx + size] for idx in range(0, len(records), size)]

    @staticmethod
    def _parse_hierarchy_path(
        raw_path_json: str, function_name: str, series_name: str, line_code: str
    ) -> list[str]:
        try:
            parsed = json.loads(str(raw_path_json or "[]"))
        except json.JSONDecodeError:
            parsed = []
        if isinstance(parsed, list):
            path = [str(item).strip() for item in parsed if str(item).strip()]
            if path:
                return path

        fallback_leaf = str(function_name).strip() or str(series_name).strip() or str(line_code).strip()
        return [fallback_leaf] if fallback_leaf else []

    def _sync_series_hierarchy(
        self,
        conn: Any,
        *,
        schema_gold: str,
        source_id: int,
        table_name: str,
    ) -> None:
        series_rows = conn.execute(
            text(
                f"""
                SELECT
                    series_id,
                    line_code,
                    series_name,
                    function_name,
                    hierarchy_path_json
                FROM {schema_gold}.dim_series
                WHERE source_id = :source_id
                  AND bea_table_name = :table_name;
                """
            ),
            {"source_id": source_id, "table_name": table_name},
        ).fetchall()
        if not series_rows:
            return

        node_rows = conn.execute(
            text(
                f"""
                SELECT node_id, node_key
                FROM {schema_gold}.dim_series_node
                WHERE source_id = :source_id
                  AND bea_table_name = :table_name;
                """
            ),
            {"source_id": source_id, "table_name": table_name},
        ).fetchall()
        node_key_to_id = {str(node_key): int(node_id) for node_id, node_key in node_rows}

        series_ids = [int(row.series_id) for row in series_rows]
        conn.execute(
            text(
                f"""
                DELETE FROM {schema_gold}.bridge_series_node
                WHERE series_id = ANY(:series_ids);
                """
            ),
            {"series_ids": series_ids},
        )

        bridge_params: list[dict[str, Any]] = []
        for row in series_rows:
            series_id = int(row.series_id)
            path = self._parse_hierarchy_path(
                str(row.hierarchy_path_json or "[]"),
                str(row.function_name or ""),
                str(row.series_name or ""),
                str(row.line_code or ""),
            )
            parent_id: int | None = None
            node_path_parts: list[str] = []
            for level, node_label in enumerate(path, start=1):
                node_path_parts.append(node_label)
                node_key = "||".join(node_path_parts)
                node_id = node_key_to_id.get(node_key)
                if node_id is None:
                    inserted = conn.execute(
                        text(
                            f"""
                            INSERT INTO {schema_gold}.dim_series_node (
                                source_id,
                                bea_table_name,
                                node_key,
                                node_label,
                                node_level,
                                parent_node_id
                            )
                            VALUES (
                                :source_id,
                                :bea_table_name,
                                :node_key,
                                :node_label,
                                :node_level,
                                :parent_node_id
                            )
                            ON CONFLICT (source_id, bea_table_name, node_key)
                            DO UPDATE
                            SET
                                node_label = EXCLUDED.node_label,
                                node_level = EXCLUDED.node_level,
                                parent_node_id = EXCLUDED.parent_node_id,
                                updated_at = NOW()
                            RETURNING node_id;
                            """
                        ),
                        {
                            "source_id": source_id,
                            "bea_table_name": table_name,
                            "node_key": node_key,
                            "node_label": node_label,
                            "node_level": level,
                            "parent_node_id": parent_id,
                        },
                    ).first()
                    node_id = int(inserted[0])
                    node_key_to_id[node_key] = node_id
                parent_id = node_id
                bridge_params.append(
                    {
                        "series_id": series_id,
                        "node_id": node_id,
                        "path_ordinal": level,
                        "is_leaf": level == len(path),
                    }
                )

        if bridge_params:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {schema_gold}.bridge_series_node (
                        series_id,
                        node_id,
                        path_ordinal,
                        is_leaf
                    )
                    VALUES (
                        :series_id,
                        :node_id,
                        :path_ordinal,
                        :is_leaf
                    )
                    ON CONFLICT (series_id, node_id)
                    DO UPDATE
                    SET
                        path_ordinal = EXCLUDED.path_ordinal,
                        is_leaf = EXCLUDED.is_leaf;
                    """
                ),
                bridge_params,
            )

    def upsert_conformed_observations(
        self,
        conformed_frame: pd.DataFrame,
        *,
        run_id: str,
        source_release_tag: str | None,
    ) -> None:
        if conformed_frame.empty:
            raise ValueError("Conformed frame is empty; refusing load.")

        schema_gold = self._validate_identifier(self.schema_gold)
        required = [
            "source_name",
            "dataset_id",
            "bea_table_name",
            "frequency",
            "period_code",
            "year",
            "month",
            "quarter",
            "state_fips",
            "state_abbrev",
            "geo_name",
            "line_code",
            "series_code",
            "series_name",
            "function_name",
            "raw_description",
            "hierarchy_path_json",
            "unit",
            "unit_mult",
            "vintage_tag",
            "pce_value",
            "pce_value_scaled",
            "note_ref",
        ]
        missing = [col for col in required if col not in conformed_frame.columns]
        if missing:
            raise ValueError(f"Conformed frame missing required columns: {missing}")

        source_name = str(conformed_frame["source_name"].iloc[0]).strip().upper()
        dataset_id = str(conformed_frame["dataset_id"].iloc[0]).strip()
        table_name = str(conformed_frame["bea_table_name"].iloc[0]).strip().upper()
        vintage_tag = str(conformed_frame["vintage_tag"].iloc[0]).strip()
        as_of_timestamp = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            source_row = conn.execute(
                text(
                    f"""
                    INSERT INTO {schema_gold}.dim_source (source_name, dataset_id, table_name)
                    VALUES (:source_name, :dataset_id, :table_name)
                    ON CONFLICT (source_name, dataset_id, table_name)
                    DO UPDATE SET updated_at = NOW()
                    RETURNING source_id;
                    """
                ),
                {
                    "source_name": source_name,
                    "dataset_id": dataset_id,
                    "table_name": table_name,
                },
            ).first()
            source_id = int(source_row[0])

            conn.execute(
                text(
                    f"""
                    UPDATE {schema_gold}.dim_vintage
                    SET is_latest = FALSE, updated_at = NOW()
                    WHERE source_id = :source_id AND vintage_tag <> :vintage_tag;
                    """
                ),
                {"source_id": source_id, "vintage_tag": vintage_tag},
            )
            vintage_row = conn.execute(
                text(
                    f"""
                    INSERT INTO {schema_gold}.dim_vintage (
                        source_id,
                        vintage_tag,
                        release_tag,
                        as_of_timestamp,
                        is_latest
                    )
                    VALUES (:source_id, :vintage_tag, :release_tag, :as_of_timestamp, TRUE)
                    ON CONFLICT (source_id, vintage_tag)
                    DO UPDATE
                    SET
                        release_tag = EXCLUDED.release_tag,
                        as_of_timestamp = EXCLUDED.as_of_timestamp,
                        is_latest = TRUE,
                        updated_at = NOW()
                    RETURNING vintage_id;
                    """
                ),
                {
                    "source_id": source_id,
                    "vintage_tag": vintage_tag,
                    "release_tag": source_release_tag,
                    "as_of_timestamp": as_of_timestamp,
                },
            ).first()
            vintage_id = int(vintage_row[0])

            geos = conformed_frame[["state_fips", "state_abbrev", "geo_name"]].drop_duplicates()
            geo_params: list[dict[str, Any]] = []
            for row in geos.itertuples(index=False):
                geo_params.append(
                    {
                        "state_fips": str(row.state_fips),
                        "state_abbrev": str(row.state_abbrev),
                        "geo_name": str(row.geo_name),
                    }
                )
            if geo_params:
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {schema_gold}.dim_geo (
                            geo_level,
                            country_code,
                            state_fips,
                            state_abbrev,
                            geo_name
                        )
                        VALUES ('state', 'US', :state_fips, :state_abbrev, :geo_name)
                        ON CONFLICT (geo_level, country_code, state_fips)
                        DO UPDATE
                        SET
                            state_abbrev = EXCLUDED.state_abbrev,
                            geo_name = EXCLUDED.geo_name,
                            updated_at = NOW();
                        """
                    ),
                    geo_params,
                )

            periods = conformed_frame[["frequency", "period_code", "year"]].drop_duplicates()
            period_params: list[dict[str, Any]] = []
            for row in periods.itertuples(index=False):
                period_rows = conformed_frame[
                    (conformed_frame["frequency"] == row.frequency)
                    & (conformed_frame["period_code"] == row.period_code)
                ][["month", "quarter"]]
                month_value: int | None = None
                quarter_value: int | None = None
                if not period_rows.empty:
                    month_raw = period_rows["month"].dropna()
                    quarter_raw = period_rows["quarter"].dropna()
                    if not month_raw.empty:
                        month_value = int(month_raw.iloc[0])
                    if not quarter_raw.empty:
                        quarter_value = int(quarter_raw.iloc[0])
                period_start, period_end, month_value, quarter_value = self._period_bounds(
                    str(row.frequency),
                    str(row.period_code),
                    int(row.year),
                    month_value,
                    quarter_value,
                )
                period_params.append(
                    {
                        "frequency": str(row.frequency),
                        "period_code": str(row.period_code),
                        "period_start_date": period_start,
                        "period_end_date": period_end,
                        "year": int(row.year),
                        "month": month_value,
                        "quarter": quarter_value,
                    }
                )
            if period_params:
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {schema_gold}.dim_period (
                            frequency,
                            period_code,
                            period_start_date,
                            period_end_date,
                            year,
                            quarter,
                            month,
                            is_period_end
                        )
                        VALUES (
                            :frequency,
                            :period_code,
                            :period_start_date,
                            :period_end_date,
                            :year,
                            :quarter,
                            :month,
                            TRUE
                        )
                        ON CONFLICT (frequency, period_code)
                        DO UPDATE
                        SET
                            period_start_date = EXCLUDED.period_start_date,
                            period_end_date = EXCLUDED.period_end_date,
                            year = EXCLUDED.year,
                            updated_at = NOW();
                        """
                    ),
                    period_params,
                )

            series = conformed_frame[
                [
                    "bea_table_name",
                    "line_code",
                    "series_code",
                    "series_name",
                    "function_name",
                    "raw_description",
                    "hierarchy_path_json",
                    "unit",
                    "unit_mult",
                ]
            ].drop_duplicates()
            series_params: list[dict[str, Any]] = []
            for row in series.itertuples(index=False):
                series_params.append(
                    {
                        "source_id": source_id,
                        "bea_table_name": str(row.bea_table_name),
                        "line_code": str(row.line_code),
                        "series_code": str(row.series_code),
                        "series_name": str(row.series_name),
                        "function_name": str(row.function_name),
                        "raw_description": str(row.raw_description),
                        "hierarchy_path_json": str(row.hierarchy_path_json),
                        "parse_strategy": "colon_path",
                        "unit": str(row.unit),
                        "unit_mult": int(row.unit_mult),
                    }
                )
            if series_params:
                conn.execute(
                    text(
                        f"""
                        INSERT INTO {schema_gold}.dim_series (
                            source_id,
                            bea_table_name,
                            line_code,
                            series_code,
                            series_name,
                            function_name,
                            raw_description,
                            hierarchy_path_json,
                            parse_strategy,
                            unit,
                            unit_mult
                        )
                        VALUES (
                            :source_id,
                            :bea_table_name,
                            :line_code,
                            :series_code,
                            :series_name,
                            :function_name,
                            :raw_description,
                            :hierarchy_path_json,
                            :parse_strategy,
                            :unit,
                            :unit_mult
                        )
                        ON CONFLICT (source_id, series_code)
                        DO UPDATE
                        SET
                            bea_table_name = EXCLUDED.bea_table_name,
                            line_code = EXCLUDED.line_code,
                            series_name = EXCLUDED.series_name,
                            function_name = EXCLUDED.function_name,
                            raw_description = EXCLUDED.raw_description,
                            hierarchy_path_json = EXCLUDED.hierarchy_path_json,
                            parse_strategy = EXCLUDED.parse_strategy,
                            unit = EXCLUDED.unit,
                            unit_mult = EXCLUDED.unit_mult,
                            updated_at = NOW();
                        """
                    ),
                    series_params,
                )
            self._sync_series_hierarchy(
                conn,
                schema_gold=schema_gold,
                source_id=source_id,
                table_name=table_name,
            )

            geo_rows = conn.execute(
                text(
                    f"""
                    SELECT geo_id, state_fips
                    FROM {schema_gold}.dim_geo
                    WHERE geo_level = 'state'
                      AND country_code = 'US';
                    """
                )
            ).fetchall()
            geo_map = {str(state_fips): int(geo_id) for geo_id, state_fips in geo_rows}

            period_rows = conn.execute(
                text(
                    f"""
                    SELECT period_id, frequency, period_code
                    FROM {schema_gold}.dim_period;
                    """
                )
            ).fetchall()
            period_map = {(str(freq), str(code)): int(period_id) for period_id, freq, code in period_rows}

            series_rows = conn.execute(
                text(
                    f"""
                    SELECT series_id, series_code
                    FROM {schema_gold}.dim_series
                    WHERE source_id = :source_id;
                    """
                ),
                {"source_id": source_id},
            ).fetchall()
            series_map = {str(series_code): int(series_id) for series_id, series_code in series_rows}

            fact_sql = text(
                f"""
                INSERT INTO {schema_gold}.fact_macro_observation (
                    source_id,
                    series_id,
                    geo_id,
                    period_id,
                    vintage_id,
                    value_numeric,
                    value_scaled,
                    note_ref,
                    run_id
                )
                VALUES (
                    :source_id,
                    :series_id,
                    :geo_id,
                    :period_id,
                    :vintage_id,
                    :value_numeric,
                    :value_scaled,
                    :note_ref,
                    :run_id
                )
                ON CONFLICT (source_id, series_id, geo_id, period_id, vintage_id)
                DO UPDATE
                SET
                    value_numeric = EXCLUDED.value_numeric,
                    value_scaled = EXCLUDED.value_scaled,
                    note_ref = EXCLUDED.note_ref,
                    run_id = EXCLUDED.run_id,
                    loaded_at = NOW();
                """
            )

            fact_params: list[dict[str, Any]] = []
            for row in conformed_frame.itertuples(index=False):
                geo_id = geo_map.get(str(row.state_fips))
                period_id = period_map.get((str(row.frequency), str(row.period_code)))
                series_id = series_map.get(str(row.series_code))
                if geo_id is None or period_id is None or series_id is None:
                    raise ValueError(
                        "Failed to resolve conformed foreign keys for row "
                        f"state_fips={row.state_fips} period_code={row.period_code} "
                        f"series_code={row.series_code}"
                    )
                fact_params.append(
                    {
                        "source_id": source_id,
                        "series_id": series_id,
                        "geo_id": geo_id,
                        "period_id": period_id,
                        "vintage_id": vintage_id,
                        "value_numeric": float(row.pce_value),
                        "value_scaled": float(row.pce_value_scaled),
                        "note_ref": str(row.note_ref) if row.note_ref is not None else None,
                        "run_id": run_id,
                    }
                )
            for chunk in self._chunked(fact_params, size=5000):
                conn.execute(fact_sql, chunk)

    def create_or_replace_views(self) -> None:
        schema_gold = self._validate_identifier(self.schema_gold)
        schema_serving = self._validate_identifier(self.schema_serving)
        view_sql = f"""
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
        with self.engine.begin() as conn:
            conn.execute(text(view_sql))

    def record_run(self, run_id: str, stage: str, status: str, details: dict[str, Any]) -> None:
        schema_meta = self._validate_identifier(self.schema_meta)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {schema_meta}.ingest_runs (run_id, stage, status, details)
                    VALUES (:run_id, :stage, :status, CAST(:details AS JSONB))
                    ON CONFLICT (run_id) DO UPDATE
                    SET
                        stage = EXCLUDED.stage,
                        status = EXCLUDED.status,
                        details = EXCLUDED.details;
                    """
                ),
                {
                    "run_id": run_id,
                    "stage": stage,
                    "status": status,
                    "details": json.dumps(details, sort_keys=True),
                },
            )
