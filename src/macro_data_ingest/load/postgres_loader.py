from __future__ import annotations

import pandas as pd


class PostgresLoader:
    """Scaffold for idempotent upserts and serving view refreshes."""

    def __init__(self, dsn: str, schema_gold: str, schema_meta: str) -> None:
        self.dsn = dsn
        self.schema_gold = schema_gold
        self.schema_meta = schema_meta

    def upsert_gold_table(self, table_name: str, frame: pd.DataFrame, pk_cols: list[str]) -> None:
        raise NotImplementedError("Postgres load is not implemented yet.")

    def record_run(self, run_id: str, status: str, details: dict) -> None:
        raise NotImplementedError("Run metadata persistence is not implemented yet.")
