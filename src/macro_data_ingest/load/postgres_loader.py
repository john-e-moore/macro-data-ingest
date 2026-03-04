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

        CREATE TABLE IF NOT EXISTS {schema_gold}.pce_state_annual (
            bea_table_name TEXT NOT NULL,
            state_fips TEXT NOT NULL,
            state_abbrev TEXT NOT NULL,
            geo_name TEXT NOT NULL,
            frequency TEXT NOT NULL,
            period_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NULL,
            quarter INTEGER NULL,
            line_code TEXT NOT NULL,
            series_code TEXT NOT NULL,
            series_name TEXT NOT NULL,
            function_name TEXT NOT NULL,
            pce_value DOUBLE PRECISION NOT NULL,
            pce_value_scaled DOUBLE PRECISION NOT NULL,
            unit TEXT NOT NULL,
            unit_mult INTEGER NOT NULL,
            note_ref TEXT NULL,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (bea_table_name, state_fips, year, line_code)
        );

        CREATE TABLE IF NOT EXISTS {schema_gold}.pce_state_monthly (
            bea_table_name TEXT NOT NULL,
            state_fips TEXT NOT NULL,
            state_abbrev TEXT NOT NULL,
            geo_name TEXT NOT NULL,
            frequency TEXT NOT NULL,
            period_code TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            quarter INTEGER NULL,
            line_code TEXT NOT NULL,
            series_code TEXT NOT NULL,
            series_name TEXT NOT NULL,
            function_name TEXT NOT NULL,
            pce_value DOUBLE PRECISION NOT NULL,
            pce_value_scaled DOUBLE PRECISION NOT NULL,
            unit TEXT NOT NULL,
            unit_mult INTEGER NOT NULL,
            note_ref TEXT NULL,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (bea_table_name, state_fips, period_code, line_code)
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
            unit TEXT NOT NULL,
            unit_mult INTEGER NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE (source_id, series_code)
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
                            unit = EXCLUDED.unit,
                            unit_mult = EXCLUDED.unit_mult,
                            updated_at = NOW();
                        """
                    ),
                    series_params,
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

    def upsert_gold_table(self, table_name: str, frame: pd.DataFrame, pk_cols: list[str]) -> None:
        if frame.empty:
            raise ValueError("Gold frame is empty; refusing load.")

        table_name = self._validate_identifier(table_name)
        schema_gold = self._validate_identifier(self.schema_gold)
        for col in frame.columns:
            self._validate_identifier(col)
        for col in pk_cols:
            self._validate_identifier(col)

        temp_table = f"tmp_{table_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        temp_table = self._validate_identifier(temp_table)
        cols = list(frame.columns)
        cols_csv = ", ".join(cols)
        update_cols = [col for col in cols if col not in pk_cols]
        update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
        pk_csv = ", ".join(pk_cols)

        with self.engine.begin() as conn:
            frame.to_sql(temp_table, conn, schema=schema_gold, if_exists="replace", index=False)
            upsert_sql = f"""
            INSERT INTO {schema_gold}.{table_name} ({cols_csv})
            SELECT {cols_csv}
            FROM {schema_gold}.{temp_table}
            ON CONFLICT ({pk_csv}) DO UPDATE
            SET {update_set};
            DROP TABLE {schema_gold}.{temp_table};
            """
            conn.execute(text(upsert_sql))

    def create_or_replace_views(self) -> None:
        schema_gold = self._validate_identifier(self.schema_gold)
        schema_serving = self._validate_identifier(self.schema_serving)
        view_sql = f"""
        DROP VIEW IF EXISTS {schema_serving}.v_pce_state_yoy;
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

        CREATE VIEW {schema_serving}.v_pce_state_yoy AS
        SELECT
            bea_table_name,
            state_fips,
            state_abbrev,
            geo_name,
            line_code,
            year,
            value_current AS pce_value_current,
            value_prior AS pce_value_prior,
            yoy_pct
        FROM {schema_serving}.v_macro_yoy
        WHERE source_name = 'BEA';
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
