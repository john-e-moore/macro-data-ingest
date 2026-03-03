from macro_data_ingest.ingest.pipeline import _is_changed


def test_is_changed_when_no_checkpoint() -> None:
    assert _is_changed(None, "abc")


def test_is_changed_when_hash_matches() -> None:
    checkpoint = {"payload_hash": "abc"}
    assert not _is_changed(checkpoint, "abc")


def test_is_changed_when_hash_differs() -> None:
    checkpoint = {"payload_hash": "abc"}
    assert _is_changed(checkpoint, "def")
