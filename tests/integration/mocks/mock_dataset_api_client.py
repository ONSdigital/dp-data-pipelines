from unittest.mock import MagicMock
from dpytools.http.api.dataset_api_client import DatasetAPIClient


class MockDatasetAPIClientConfig:
    def __init__(
        self, mock_get_response, mock_get_path_response, mock_post_json_response
    ):
        self.mock_get_response = mock_get_response
        self.mock_get_path_response = mock_get_path_response
        self.mock_post_json_response = mock_post_json_response


def raise_for_status(mock: MagicMock):
    if mock.status_code > 299 or mock.status_code < 200:
        raise Exception(f"Mock error raised as mock status_code is {mock.status_code}")


class MockDatasetAPIClient(DatasetAPIClient):
    def __init__(
        self,
        dataset_api_url: str,
        dataset_path: str,
        edition_path: str,
        config: MockDatasetAPIClientConfig,
    ):
        super().__init__(
            dataset_api_url=dataset_api_url,
            dataset_path=dataset_path,
            edition_path=edition_path,
        )
        self.mock_config = config
        self.get_path = MagicMock()
        self.get_path.return_value = self.mock_config.mock_get_path_response

        self.get = MagicMock()
        self.get.return_value = self.mock_config.mock_get_response

        self.post_json = MagicMock()
        self.post_json.return_value = self.mock_config.mock_post_json_response

        for mock_response in [
            self.post_json.return_value,
            self.get_path.return_value,
            self.get.return_value,
        ]:
            self.add_raise_for_status(mock_response)

    def add_raise_for_status(self, mock: MagicMock):
        mock.raise_for_status.side_effect = lambda: raise_for_status(mock)
