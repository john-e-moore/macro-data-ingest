from macro_data_ingest.ingest.bea_client import BeaClient, BeaQuery


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
    client._session = DummySession()

    try:
        client.fetch(BeaQuery(dataset="Regional", table_name="BAD"))
        assert False, "Expected ValueError for BEA error payload"
    except ValueError as exc:
        assert "Invalid request" in str(exc)
