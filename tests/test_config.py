from macro_data_ingest.config import load_config


def test_load_config_defaults() -> None:
    cfg = load_config()
    assert cfg.log_level
    assert cfg.pg_port > 0
    assert cfg.datasets_config_path
