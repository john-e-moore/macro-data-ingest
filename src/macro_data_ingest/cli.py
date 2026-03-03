from __future__ import annotations

import argparse
import logging
import uuid

from macro_data_ingest.config import load_config
from macro_data_ingest.ingest.pipeline import run_ingest
from macro_data_ingest.logging_utils import configure_logging
from macro_data_ingest.transforms.pipeline import run_transform

LOGGER = logging.getLogger(__name__)


def _base_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mdi", description="Macro Data Ingest CLI", add_help=False)
    parser.add_argument("--env", default="staging", choices=["staging", "prod"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--smoke", action="store_true", help="Use tiny pull and reduced workload.")
    return parser


def _resolve_run_id(run_id: str | None) -> str:
    return run_id if run_id else f"run-{uuid.uuid4()}"


def cmd_ingest(args: argparse.Namespace) -> int:
    config = load_config()
    run_id = _resolve_run_id(args.run_id)
    try:
        result = run_ingest(config=config, run_id=run_id, smoke=args.smoke)
    except Exception:
        LOGGER.exception("ingest failed", extra={"run_id": run_id, "stage": "ingest"})
        return 1

    print(
        "ingest completed "
        f"run_id={result.run_id} changed={result.changed} "
        f"rows={result.row_count} manifest={result.manifest_uri}"
    )
    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    config = load_config()
    run_id = _resolve_run_id(args.run_id)
    try:
        result = run_transform(config=config, run_id=run_id, smoke=args.smoke)
    except Exception:
        LOGGER.exception("transform failed", extra={"run_id": run_id, "stage": "transform"})
        return 1

    print(
        "transform completed "
        f"run_id={result.run_id} rows={result.row_count} "
        f"silver={result.silver_uri} manifest={result.manifest_uri}"
    )
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    run_id = _resolve_run_id(args.run_id)
    print(f"[SCAFFOLD] load env={args.env} run_id={run_id} smoke={args.smoke}")
    return 0


def cmd_run_all(args: argparse.Namespace) -> int:
    ingest_rc = cmd_ingest(args)
    if ingest_rc != 0:
        return ingest_rc
    transform_rc = cmd_transform(args)
    if transform_rc != 0:
        return transform_rc
    return cmd_load(args)


def main() -> int:
    config = load_config()
    configure_logging(config.log_level)

    parser = argparse.ArgumentParser(prog="mdi", description="Macro Data Ingest CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", parents=[_base_parser()], add_help=True)
    ingest_parser.set_defaults(handler=cmd_ingest)

    transform_parser = subparsers.add_parser("transform", parents=[_base_parser()], add_help=True)
    transform_parser.set_defaults(handler=cmd_transform)

    load_parser = subparsers.add_parser("load", parents=[_base_parser()], add_help=True)
    load_parser.set_defaults(handler=cmd_load)

    run_all_parser = subparsers.add_parser("run-all", parents=[_base_parser()], add_help=True)
    run_all_parser.set_defaults(handler=cmd_run_all)

    args = parser.parse_args()
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
