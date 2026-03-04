#!/usr/bin/env python
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import create_engine, text

from macro_data_ingest.config import AppConfig, load_config
from macro_data_ingest.ingest.bea_client import BeaClient
from macro_data_ingest.load.postgres_loader import PostgresLoader


@dataclass(frozen=True)
class BackfillResult:
    bea_table_name: str
    mapping_size: int
    empty_before: int
    empty_after: int
    rows_updated: int


def _build_dsn(config: AppConfig) -> str:
    if not config.pg_host:
        raise ValueError("PG_HOST is required for backfill.")
    if not config.pg_user or not config.pg_password:
        raise ValueError("PG_USER and PG_PASSWORD are required for backfill.")
    return (
        f"postgresql+psycopg://{config.pg_user}:{config.pg_password}"
        f"@{config.pg_host}:{config.pg_port}/{config.pg_database}"
    )


def _parse_table_names(raw: str) -> list[str]:
    names = [item.strip().upper() for item in raw.split(",") if item.strip()]
    if not names:
        raise ValueError("table names cannot be empty.")
    return names


def _default_run_id() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"backfill-function-name-{stamp}"


def _count_empty(
    engine, schema_gold: str, bea_table_name: str  # noqa: ANN001
) -> int:
    sql = text(
        f"""
        SELECT COUNT(*)
        FROM {schema_gold}.pce_state_annual
        WHERE bea_table_name = :bea_table_name
          AND COALESCE(function_name, '') = ''
        """
    )
    with engine.begin() as conn:
        return int(conn.execute(sql, {"bea_table_name": bea_table_name}).scalar_one())


def _update_table(
    engine, schema_gold: str, bea_table_name: str, mapping: dict[str, str], force: bool  # noqa: ANN001
) -> int:
    update_sql = text(
        f"""
        UPDATE {schema_gold}.pce_state_annual
        SET function_name = :function_name
        WHERE bea_table_name = :bea_table_name
          AND line_code = :line_code
          AND (:force OR COALESCE(function_name, '') = '')
        """
    )
    params = [
        {
            "bea_table_name": bea_table_name,
            "line_code": line_code,
            "function_name": function_name,
            "force": force,
        }
        for line_code, function_name in mapping.items()
    ]
    rows_updated = 0
    with engine.begin() as conn:
        for param in params:
            result = conn.execute(update_sql, param)
            rows_updated += int(result.rowcount or 0)
    return rows_updated


def run_backfill(
    config: AppConfig,
    bea_table_names: Iterable[str],
    run_id: str,
    force: bool,
    dry_run: bool,
) -> list[BackfillResult]:
    schema_gold = PostgresLoader._validate_identifier(config.pg_schema_gold)
    schema_meta = PostgresLoader._validate_identifier(config.pg_schema_meta)
    dsn = _build_dsn(config)
    engine = create_engine(dsn, connect_args={"connect_timeout": 10})
    client = BeaClient(api_key=config.bea_api_key)

    results: list[BackfillResult] = []
    for bea_table_name in bea_table_names:
        mapping = client.fetch_line_code_descriptions(config.bea_dataset, bea_table_name)
        empty_before = _count_empty(engine, schema_gold, bea_table_name)
        rows_updated = 0
        if not dry_run:
            rows_updated = _update_table(engine, schema_gold, bea_table_name, mapping, force=force)
        empty_after = _count_empty(engine, schema_gold, bea_table_name)
        results.append(
            BackfillResult(
                bea_table_name=bea_table_name,
                mapping_size=len(mapping),
                empty_before=empty_before,
                empty_after=empty_after,
                rows_updated=rows_updated,
            )
        )

    loader = PostgresLoader(
        dsn=dsn,
        schema_gold=schema_gold,
        schema_meta=schema_meta,
    )
    loader.record_run(
        run_id=f"{run_id}:function_name_backfill",
        stage="backfill",
        status="dry-run" if dry_run else "success",
        details={
            "run_id": run_id,
            "dataset": "pce_state_annual",
            "table_names": list(bea_table_names),
            "force": force,
            "results": [result.__dict__ for result in results],
        },
    )
    return results


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backfill function_name values in gold.pce_state_annual.")
    parser.add_argument("--env", choices=["staging", "prod"], default="staging")
    parser.add_argument("--tables", default="SAPCE3,SAPCE4")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--force", action="store_true", help="Overwrite non-empty function_name values.")
    parser.add_argument("--dry-run", action="store_true", help="Compute stats only; do not update rows.")
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    config = load_config()
    if config.app_env != args.env:
        print(
            f"warning: APP_ENV={config.app_env} while --env={args.env}; using current env variables as loaded."
        )
    if not config.bea_api_key:
        raise ValueError("BEA_API_KEY is required for function_name backfill.")
    run_id = args.run_id or _default_run_id()
    tables = _parse_table_names(args.tables)
    results = run_backfill(
        config=config,
        bea_table_names=tables,
        run_id=run_id,
        force=args.force,
        dry_run=args.dry_run,
    )
    for result in results:
        print(
            "function_name_backfill "
            f"table={result.bea_table_name} "
            f"mapping_size={result.mapping_size} "
            f"empty_before={result.empty_before} "
            f"rows_updated={result.rows_updated} "
            f"empty_after={result.empty_after}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
