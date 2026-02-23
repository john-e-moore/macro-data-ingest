from macro_data_ingest.cli import _resolve_run_id


def test_run_id_resolution() -> None:
    explicit = _resolve_run_id("run-123")
    generated = _resolve_run_id(None)

    assert explicit == "run-123"
    assert generated.startswith("run-")
