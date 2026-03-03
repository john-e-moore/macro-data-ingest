from argparse import Namespace

from macro_data_ingest import cli
from macro_data_ingest.datasets import BeaDatasetSpec
from macro_data_ingest.ingest.pipeline import IngestResult


def test_run_all_skips_downstream_when_unchanged(monkeypatch) -> None:  # noqa: ANN001
    ingest_result = IngestResult(
        run_id="run-1",
        changed=False,
        payload_hash="abc",
        row_count=10,
        raw_payload_uri=None,
        manifest_uri="s3://bucket/manifest.json",
        checkpoint_uri="s3://bucket/checkpoint.json",
    )

    dataset_spec = BeaDatasetSpec(
        dataset_id="pce_state_sapce4",
        source="bea",
        storage_dataset="pce_state",
        bea_dataset="Regional",
        bea_table_name="SAPCE4",
        bea_frequency="A",
        bea_start_year=2000,
        line_code="ALL",
        geo_fips="STATE",
        target_table="pce_state_annual",
        enabled=True,
    )

    def fake_ingest_stage(args, config, dataset_spec, run_id):  # noqa: ANN001, ANN202
        del args, config, dataset_spec, run_id
        return ingest_result

    def fail_transform(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("transform should not run when ingest is unchanged")

    def fail_load(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("load should not run when ingest is unchanged")

    monkeypatch.setattr(cli, "_run_ingest_stage", fake_ingest_stage)
    monkeypatch.setattr(cli, "_resolve_specs", lambda args: [dataset_spec])
    monkeypatch.setattr(cli, "run_transform", fail_transform)
    monkeypatch.setattr(cli, "run_load", fail_load)

    args = Namespace(run_id="run-1", smoke=False, env="staging", dataset_id=None)
    assert cli.cmd_run_all(args) == 0
