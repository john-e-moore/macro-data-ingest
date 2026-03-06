from macro_data_ingest.ingest.bea_client import BeaClient, BeaQuery
import requests


def test_build_params_contains_required_keys() -> None:
    client = BeaClient(api_key="abc-123")
    query = BeaQuery(dataset="Regional", table_name="SQPCE", frequency="A", year="2024")

    params = client._build_params(query)

    assert params["UserID"] == "abc-123"
    assert params["method"] == "GetData"
    assert params["datasetname"] == "Regional"
    assert params["TableName"] == "SQPCE"
    assert params["ResultFormat"] == "JSON"


def test_extract_rows_reads_bea_results_data() -> None:
    payload = {"BEAAPI": {"Results": {"Data": [{"GeoFips": "00000", "TimePeriod": "2024"}]}}}
    rows = BeaClient.extract_rows(payload)
    assert len(rows) == 1
    assert rows[0]["TimePeriod"] == "2024"


def test_build_params_omits_line_code_for_all() -> None:
    client = BeaClient(api_key="abc-123")
    params = client._build_params(BeaQuery(dataset="Regional", table_name="SAPCE4", line_code="ALL"))
    assert "LineCode" not in params


def test_fetch_raises_when_bea_error() -> None:
    class DummyResponse:
        text = "{}"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "BEAAPI": {
                    "Error": {
                        "APIErrorDescription": "Invalid request",
                        "ErrorDetail": {"Description": "Invalid Value for Parameter TableName"},
                    }
                }
            }

    class DummySession:
        def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return DummyResponse()

    client = BeaClient(api_key="abc-123")
    client._http._session = DummySession()

    try:
        client.fetch(BeaQuery(dataset="Regional", table_name="BAD"))
        assert False, "Expected ValueError for BEA error payload"
    except ValueError as exc:
        assert "Invalid request" in str(exc)


def test_fetch_line_codes_reads_parameter_values() -> None:
    class DummyResponse:
        text = "{}"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "BEAAPI": {
                    "Results": {
                        "ParamValue": [
                            {"Key": "1", "Desc": "A"},
                            {"Key": "10", "Desc": "B"},
                        ]
                    }
                }
            }

    class DummySession:
        def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return DummyResponse()

    client = BeaClient(api_key="abc-123")
    client._http._session = DummySession()
    assert client.fetch_line_codes("Regional", "SAPCE4") == ["1", "10"]


def test_fetch_line_code_descriptions_reads_desc_values() -> None:
    class DummyResponse:
        text = "{}"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {
                "BEAAPI": {
                    "Results": {
                        "ParamValue": [
                            {"Key": "1", "Desc": "Personal consumption expenditures"},
                            {"Key": "10", "Desc": "Food services and accommodations"},
                        ]
                    }
                }
            }

    class DummySession:
        def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return DummyResponse()

    client = BeaClient(api_key="abc-123")
    client._http._session = DummySession()
    assert client.fetch_line_code_descriptions("Regional", "SAPCE4") == {
        "1": "Personal consumption expenditures",
        "10": "Food services and accommodations",
    }


def test_fetch_retries_transient_429() -> None:
    class DummyResponse429:
        status_code = 429
        headers = {"Retry-After": "0"}
        text = "{}"

        def raise_for_status(self) -> None:
            raise requests.HTTPError(response=self)

        def json(self) -> dict:
            return {}

    class DummyResponseOK:
        text = "{}"

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return {"BEAAPI": {"Results": {"Data": [{"x": 1}]}}}

    class DummySession:
        def __init__(self) -> None:
            self.calls = 0

        def get(self, *args, **kwargs):  # noqa: ANN002, ANN003
            self.calls += 1
            if self.calls == 1:
                return DummyResponse429()
            return DummyResponseOK()

    client = BeaClient(api_key="abc-123", max_retries=2, retry_backoff_seconds=0, min_request_interval_seconds=0)
    client._http._session = DummySession()
    payload = client.fetch(BeaQuery(dataset="Regional", table_name="SAPCE4", line_code="1"))
    assert payload["BEAAPI"]["Results"]["Data"][0]["x"] == 1
