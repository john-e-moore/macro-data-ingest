from __future__ import annotations

import re
import json
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
            year INTEGER NOT NULL,
            line_code TEXT NOT NULL,
            series_code TEXT NOT NULL,
            pce_value DOUBLE PRECISION NOT NULL,
            pce_value_scaled DOUBLE PRECISION NOT NULL,
            unit TEXT NOT NULL,
            unit_mult INTEGER NOT NULL,
            note_ref TEXT NULL,
            run_id TEXT NOT NULL,
            loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (bea_table_name, state_fips, year, line_code)
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(ddl))
            conn.execute(
                text(
                    f"""
                    ALTER TABLE {schema_gold}.pce_state_annual
                    ADD COLUMN IF NOT EXISTS bea_table_name TEXT;
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    UPDATE {schema_gold}.pce_state_annual
                    SET bea_table_name = split_part(series_code, '-', 1)
                    WHERE bea_table_name IS NULL;
                    """
                )
            )
            conn.execute(
                text(
                    f"""
                    ALTER TABLE {schema_gold}.pce_state_annual
                    ALTER COLUMN bea_table_name SET NOT NULL;
                    """
                )
            )
            pk_name = conn.execute(
                text(
                    """
                    SELECT conname
                    FROM pg_constraint
                    WHERE conrelid = to_regclass(:full_table_name)
                      AND contype = 'p'
                    """
                ),
                {"full_table_name": f"{schema_gold}.pce_state_annual"},
            ).scalar()
            if pk_name:
                conn.execute(
                    text(
                        f"""
                        ALTER TABLE {schema_gold}.pce_state_annual
                        DROP CONSTRAINT IF EXISTS {pk_name};
                        """
                    )
                )
            conn.execute(
                text(
                    f"""
                    ALTER TABLE {schema_gold}.pce_state_annual
                    ADD CONSTRAINT pce_state_annual_pkey PRIMARY KEY
                    (bea_table_name, state_fips, year, line_code);
                    """
                )
            )

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
        CREATE OR REPLACE VIEW {schema_serving}.v_pce_state_yoy AS
        SELECT
            cur.bea_table_name,
            cur.state_fips,
            cur.state_abbrev,
            cur.geo_name,
            cur.line_code,
            cur.year,
            cur.pce_value AS pce_value_current,
            prev.pce_value AS pce_value_prior,
            CASE
                WHEN prev.pce_value IS NULL OR prev.pce_value = 0 THEN NULL
                ELSE ((cur.pce_value - prev.pce_value) / prev.pce_value) * 100.0
            END AS yoy_pct
        FROM {schema_gold}.pce_state_annual cur
        LEFT JOIN {schema_gold}.pce_state_annual prev
            ON cur.bea_table_name = prev.bea_table_name
           AND cur.state_fips = prev.state_fips
           AND cur.line_code = prev.line_code
           AND cur.year = prev.year + 1;
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
