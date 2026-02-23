from macro_data_ingest.config import load_config


def test_load_config_defaults() -> None:
    cfg = load_config()
    assert cfg.app_env in {"staging", "prod"}
    assert cfg.pg_port > 0
