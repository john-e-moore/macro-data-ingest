from macro_data_ingest.ingest.census_client import CensusClient


def test_fetch_state_population_maps_rows() -> None:
    class DummyClient(CensusClient):
        def __init__(self) -> None:
            super().__init__(api_key="x")

        def _request(self, year: int, dataset_path: str, params: dict[str, str]) -> list[list[str]]:
            assert dataset_path == "acs/acs1"
            assert params["get"] == "NAME,B01003_001E"
            assert params["for"] == "state:*"
            return [
                ["NAME", "B01003_001E", "state"],
                ["Alabama", "5108468", "01"],
                ["Alaska", "733406", "02"],
            ]

    client = DummyClient()
    rows = client.fetch_state_population(
        years=[2024],
        dataset_path="acs/acs1",
        variable="B01003_001E",
    )
    assert len(rows) == 2
    assert rows[0]["NAME"] == "Alabama"
    assert rows[0]["state"] == "01"
    assert rows[0]["B01003_001E"] == "5108468"
    assert rows[0]["YEAR"] == "2024"
