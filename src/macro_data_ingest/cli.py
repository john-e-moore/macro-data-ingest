from __future__ import annotations

import argparse
import logging
import uuid

from macro_data_ingest.config import load_config
from macro_data_ingest.datasets import BeaDatasetSpec, load_dataset_specs
from macro_data_ingest.ingest.pipeline import IngestResult, run_ingest
from macro_data_ingest.load.pipeline import run_load
from macro_data_ingest.logging_utils import configure_logging
from macro_data_ingest.transforms.pipeline import run_transform

LOGGER = logging.getLogger(__name__)


def _base_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mdi", description="Macro Data Ingest CLI", add_help=False)
    parser.add_argument("--env", default="staging", choices=["staging", "prod"])
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--smoke", action="store_true", help="Use tiny pull and reduced workload.")
    parser.add_argument(
        "--dataset-id",
        default=None,
        help="Run a single configured dataset ID from datasets config.",
    )
    return parser


def _resolve_run_id(run_id: str | None) -> str:
    return run_id if run_id else f"run-{uuid.uuid4()}"


def _resolve_specs(config_path_args: argparse.Namespace) -> list[BeaDatasetSpec]:
    config = load_config()
    specs = load_dataset_specs(config)
    dataset_id = getattr(config_path_args, "dataset_id", None)
    if not dataset_id:
        return specs
    selected = [spec for spec in specs if spec.dataset_id == dataset_id]
    if not selected:
        raise ValueError(f"Unknown dataset-id: {dataset_id}")
    return selected


def _dataset_run_id(base_run_id: str, dataset_id: str) -> str:
    return f"{base_run_id}-{dataset_id}"


def _run_ingest_stage(
    args: argparse.Namespace,
    config,
    dataset_spec: BeaDatasetSpec,
    run_id: str,
) -> IngestResult:
    return run_ingest(config=config, run_id=run_id, dataset_spec=dataset_spec, smoke=args.smoke)


def cmd_ingest(args: argparse.Namespace) -> int:
    config = load_config()
    base_run_id = _resolve_run_id(args.run_id)
    try:
        specs = _resolve_specs(args)
    except Exception:
        LOGGER.exception("failed to resolve datasets", extra={"run_id": base_run_id, "stage": "ingest"})
        return 1
    for spec in specs:
        dataset_run_id = _dataset_run_id(base_run_id, spec.dataset_id)
        try:
            result = _run_ingest_stage(args, config=config, dataset_spec=spec, run_id=dataset_run_id)
        except Exception:
            LOGGER.exception("ingest failed", extra={"run_id": dataset_run_id, "stage": "ingest"})
            return 1
        print(
            "ingest completed "
            f"dataset_id={spec.dataset_id} run_id={result.run_id} changed={result.changed} "
            f"rows={result.row_count} manifest={result.manifest_uri}"
        )
    return 0


def cmd_transform(args: argparse.Namespace) -> int:
    config = load_config()
    base_run_id = _resolve_run_id(args.run_id)
    try:
        specs = _resolve_specs(args)
    except Exception:
        LOGGER.exception(
            "failed to resolve datasets", extra={"run_id": base_run_id, "stage": "transform"}
        )
        return 1
    for spec in specs:
        dataset_run_id = _dataset_run_id(base_run_id, spec.dataset_id)
        try:
            result = run_transform(
                config=config, run_id=dataset_run_id, dataset_spec=spec, smoke=args.smoke
            )
        except Exception:
            LOGGER.exception("transform failed", extra={"run_id": dataset_run_id, "stage": "transform"})
            return 1

        print(
            "transform completed "
            f"dataset_id={spec.dataset_id} run_id={result.run_id} rows={result.row_count} "
            f"silver={result.silver_uri} manifest={result.manifest_uri}"
        )
    return 0


def cmd_load(args: argparse.Namespace) -> int:
    config = load_config()
    base_run_id = _resolve_run_id(args.run_id)
    try:
        specs = _resolve_specs(args)
    except Exception:
        LOGGER.exception("failed to resolve datasets", extra={"run_id": base_run_id, "stage": "load"})
        return 1
    for spec in specs:
        dataset_run_id = _dataset_run_id(base_run_id, spec.dataset_id)
        try:
            result = run_load(config=config, run_id=dataset_run_id, dataset_spec=spec, smoke=args.smoke)
        except Exception:
            LOGGER.exception("load failed", extra={"run_id": dataset_run_id, "stage": "load"})
            return 1

        print(
            "load completed "
            f"dataset_id={spec.dataset_id} run_id={result.run_id} rows={result.row_count} "
            f"table={result.gold_table} manifest={result.manifest_uri}"
        )
    return 0


def cmd_run_all(args: argparse.Namespace) -> int:
    config = load_config()
    base_run_id = _resolve_run_id(args.run_id)
    try:
        specs = _resolve_specs(args)
    except Exception:
        LOGGER.exception(
            "failed to resolve datasets", extra={"run_id": base_run_id, "stage": "run-all"}
        )
        return 1

    for spec in specs:
        dataset_run_id = _dataset_run_id(base_run_id, spec.dataset_id)
        try:
            ingest_result = _run_ingest_stage(
                args,
                config=config,
                dataset_spec=spec,
                run_id=dataset_run_id,
            )
        except Exception:
            LOGGER.exception("ingest failed", extra={"run_id": dataset_run_id, "stage": "ingest"})
            return 1
        print(
            "ingest completed "
            f"dataset_id={spec.dataset_id} run_id={ingest_result.run_id} changed={ingest_result.changed} "
            f"rows={ingest_result.row_count} manifest={ingest_result.manifest_uri}"
        )
        if not ingest_result.changed:
            print(
                "run-all skipped transform/load because ingest payload is unchanged "
                f"for dataset_id={spec.dataset_id} run_id={ingest_result.run_id}"
            )
            continue

        try:
            transform_result = run_transform(
                config=config,
                run_id=dataset_run_id,
                dataset_spec=spec,
                smoke=args.smoke,
            )
        except Exception:
            LOGGER.exception(
                "transform failed", extra={"run_id": dataset_run_id, "stage": "transform"}
            )
            return 1
        print(
            "transform completed "
            f"dataset_id={spec.dataset_id} run_id={transform_result.run_id} "
            f"rows={transform_result.row_count} silver={transform_result.silver_uri} "
            f"manifest={transform_result.manifest_uri}"
        )

        try:
            load_result = run_load(
                config=config,
                run_id=dataset_run_id,
                dataset_spec=spec,
                smoke=args.smoke,
            )
        except Exception:
            LOGGER.exception("load failed", extra={"run_id": dataset_run_id, "stage": "load"})
            return 1
        print(
            "load completed "
            f"dataset_id={spec.dataset_id} run_id={load_result.run_id} rows={load_result.row_count} "
            f"table={load_result.gold_table} manifest={load_result.manifest_uri}"
        )
    return 0


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
