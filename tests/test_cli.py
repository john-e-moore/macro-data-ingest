from argparse import Namespace

from macro_data_ingest import cli
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

    def fake_ingest_stage(args, run_id):  # noqa: ANN001, ANN202
        del args, run_id
        return ingest_result

    def fail_transform(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        raise AssertionError("transform should not run when ingest is unchanged")

    monkeypatch.setattr(cli, "_run_ingest_stage", fake_ingest_stage)
    monkeypatch.setattr(cli, "cmd_transform", fail_transform)

    args = Namespace(run_id="run-1", smoke=False, env="staging")
    assert cli.cmd_run_all(args) == 0
