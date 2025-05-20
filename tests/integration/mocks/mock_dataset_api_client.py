from datetime import datetime
import json
from typing import Dict, List, Optional, Any
from tests.integration.constants import (
    dataset_api_url,
    test_dataset_id,
    test_edition_id,
)
from responses import matchers

mock_versions = [
    {
        "release_date": datetime.now().isoformat(),
        "state": "published",
        "distributions": [
            {"title": "Test distribution", "format": "csv", "file": "data.csv"}
        ],
        "quality_designation": "quality",
        "usage_notes": [{"title": "Test title", "note": "Test usage note"}],
        "version": 1,
    }
]


class DatasetApiUrlBuilder:
    """Builds URLs for Dataset API endpoints"""

    def __init__(
        self,
        base_url: str = dataset_api_url,
        dataset_id: str = test_dataset_id,
        edition_id: str = test_edition_id,
    ):
        self.base_url = base_url
        self.dataset_id = dataset_id
        self.edition_id = edition_id

    @property
    def datasets_url(self) -> str:
        return f"{self.base_url}/{self.dataset_id}"

    @property
    def versions_url(self) -> str:
        return f"{self.base_url}/{self.dataset_id}/editions/{self.edition_id}/versions"

    def custom_url(self, path: str) -> str:
        return f"{self.base_url}/{path}"


class DatasetApiResponseBuilder:
    """Builds response bodies for Dataset API endpoints"""

    @staticmethod
    def dataset_response(
        type: str = "static", state: str = "published"
    ) -> Dict[str, Any]:
        return {"current": {"type": type, "state": state}}

    @staticmethod
    def versions_response(
        items: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        if items:
            return {"items": items}

        return {}

    @staticmethod
    def error_response(message: str, code: str = "ERROR") -> Dict[str, Any]:
        return {"error": {"message": message, "code": code}}

    @staticmethod
    def create_headers() -> Dict[str, str]:
        return {"Date": datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")}


class MockDatasetApi:
    """Main class for managing dataset API mocks"""

    def __init__(self, responses):
        self.responses = responses
        self.url_builder = DatasetApiUrlBuilder()
        self.response_builder = DatasetApiResponseBuilder()
        self._active_mocks = {}

        # Set up default mocks
        self.setup_defaults()

    def setup_defaults(
        self,
        mock_get_dataset: bool = True,
        mock_get_versions: bool = True,
        mock_post_versions: bool = True,
    ):
        """Set up default mocks for common scenarios"""
        self.remove_all_mocks()
        if mock_get_dataset:
            self.mock_get_dataset()

        if mock_get_versions:
            self.mock_get_versions()

        if mock_post_versions:
            self.mock_post_versions()

    def mock_get_dataset(
        self,
        is_static: bool = True,
        state: str = "published",
        status_code: int = 200,
        custom_response: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Mock the dataset endpoint"""
        mock_key = "get_dataset"
        self._remove_existing_mock(mock_key)

        if status_code == 404:
            return self._mock_404_response(self.url_builder.datasets_url, mock_key)

        if custom_response:
            response_body = custom_response
        else:
            version_type = "static" if is_static else "filterable"
            response_body = self.response_builder.dataset_response(version_type, state)

        mock = self.responses.get(
            self.url_builder.datasets_url,
            body=json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.response_builder.create_headers(),
        )

        self._active_mocks[mock_key] = mock
        return mock

    def mock_get_versions(
        self,
        items: Optional[List[Dict[str, Any]]] = None,
        status_code: int = 200,
        custom_response: Optional[Dict[str, Any]] = None,
    ) -> Any:
        """Mock the versions endpoint"""
        mock_key = "get_versions"
        self._remove_existing_mock(mock_key)

        if status_code == 404:
            return self._mock_404_response(self.url_builder.datasets_url, mock_key)

        if custom_response:
            response_body = custom_response
        else:
            if items is None:
                items = [mock_versions[0]]
            response_body = self.response_builder.versions_response(items)

        mock = self.responses.get(
            self.url_builder.versions_url,
            body=json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.response_builder.create_headers(),
        )

        self._active_mocks[mock_key] = mock
        return mock

    def mock_post_versions(
        self,
        status_code: int = 201,
        response_body: Optional[Dict[str, Any]] = None,
        request_body: Optional[Dict[str, Any]] = None,
    ):
        mock_key = "post_versions"
        self._remove_existing_mock(mock_key)

        if status_code == 404:
            return self._mock_404_response(self.url_builder.datasets_url, mock_key)

        response_matchers = (
            [] if request_body is None else [matchers.json_params_matcher(request_body)]
        )

        mock = self.responses.post(
            self.url_builder.versions_url,
            body=None if response_body is None else json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.response_builder.create_headers(),
            match=response_matchers,
        )

        self._active_mocks[mock_key] = mock
        return mock

    def mock_custom_endpoint(
        self,
        method: str,
        url: str,
        response_body: Dict[str, Any],
        status_code: int = 200,
        mock_key: Optional[str] = None,
    ) -> Any:
        """Mock any custom endpoint"""
        if mock_key is None:
            mock_key = f"custom_{method.lower()}_{url.split('/')[-1]}"

        self._remove_existing_mock(mock_key)

        method_func = getattr(self.responses, method.lower())
        mock = method_func(
            url,
            body=json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.response_builder.create_headers(),
        )

        self._active_mocks[mock_key] = mock
        return mock

    def mock_dataset_error(
        self, status_code: int = 500, message: str = "Internal Server Error"
    ):
        """Mock dataset endpoint to return an error"""
        error_response = self.response_builder.error_response(message)
        return self.mock_get_dataset(
            custom_response=error_response, status_code=status_code
        )

    def mock_get_versions_error(
        self, status_code: int = 500, message: str = "Internal Server Error"
    ):
        """Mock versions endpoint to return an error"""
        error_response = self.response_builder.error_response(message)
        return self.mock_get_versions(
            custom_response=error_response, status_code=status_code
        )

    def mock_empty_versions(self):
        """Mock versions endpoint to return empty list"""
        return self.mock_get_versions(items=[])

    def mock_multiple_versions(self, count: int = 3):
        """Mock versions endpoint with multiple versions"""
        items = []
        for i in range(count):
            items.append(
                {
                    "release_date": f"2020-0{i + 1}-01T00:00:00.000Z",
                    "state": "published",
                    "version": str(i + 1),
                }
            )
        return self.mock_get_versions(items=items)

    def mock_draft_dataset(self):
        """Mock dataset in draft state"""
        return self.mock_get_dataset(state="draft")

    def mock_dynamic_dataset(self):
        """Mock dynamic dataset"""
        return self.mock_get_dataset(is_static=False)

    def get_mock(self, mock_key: str) -> Any:
        """Get a specific mock by key"""
        return self._active_mocks.get(mock_key)

    def remove_mock(self, mock_key: str):
        """Remove a specific mock"""
        self._remove_existing_mock(mock_key)

    def reset_all(self):
        """Reset all mocks and set up defaults again"""
        self.remove_all_mocks()
        self.setup_defaults()

    def _remove_existing_mock(self, mock_key: str):
        """Remove an existing mock if it exists"""
        if mock_key in self._active_mocks:
            try:
                self.responses.remove(self._active_mocks[mock_key])
            except ValueError:
                pass
            del self._active_mocks[mock_key]

    def remove_all_mocks(self):
        """Clear all mocks"""
        for mock_key in self._active_mocks:
            self.responses.remove(self._active_mocks[mock_key])

        self._active_mocks.clear()

    def assert_get_dataset_called(self, times: Optional[int] = None):
        """Assert that the dataset endpoint was called with a GET request"""
        self._assert_url_called(self.datasets_url, times, "get")

    def assert_get_versions_called(self, times: Optional[int] = None):
        """Assert that the versions endpoint was called with a GET request"""
        self._assert_url_called(self.versions_url, times, "get")

    def assert_post_versions_called(self, times: Optional[int] = None):
        """Assert that the versions endpoint was called with a POST request"""
        self._assert_url_called(self.versions_url, times, "post")

    def assert_url_called(
        self, url: str, times: Optional[int] = None, method: Optional[str] = None
    ):
        """Assert that a specific URL was called"""
        self._assert_url_called(url, times, method)

    def assert_no_requests(self):
        """Assert that no requests were made"""
        assert len(self.responses.calls) == 0, (
            f"Expected no requests, but {len(self.responses.calls)} were made"
        )

    def assert_all_requests_made(self):
        """Assert that all expected requests were made"""
        self.assert_get_dataset_called(times=1)
        self.assert_get_versions_called(times=1)
        self.assert_post_versions_called(times=1)

    def get_requests(
        self, url: Optional[str] = None, method: Optional[str] = None
    ) -> list:
        """Get the number of requests made to a specific URL or total"""
        matching = [
            call
            for call in self.responses.calls
            if (url is None or (url is not None and call.request.url == url))
            and (
                method is None or (method is not None and call.request.method == method)
            )
        ]

        return matching

    def get_all_requests(self) -> List[Any]:
        """Get all requests that were made"""
        return self.responses.calls

    def get_requests_to_url(self, url: str) -> List[Any]:
        """Get all requests made to a specific URL"""
        return [call for call in self.responses.calls if call.request.url == url]

    def get_dataset_requests(self) -> List[Any]:
        """Get all requests made to the dataset endpoint"""
        return self.get_requests_to_url(self.datasets_url)

    def get_versions_requests(self) -> List[Any]:
        """Get all requests made to the versions endpoint"""
        return self.get_requests_to_url(self.versions_url)

    def print_request_summary(self):
        """Print a summary of all requests made (useful for debugging)"""
        if not self.responses.calls:
            return "No requests were made"

        requests_str = f"Total requests made: {len(self.responses.calls)}"
        for i, call in enumerate(self.responses.calls, 1):
            requests_str += f"\n{i}. {call.request.method} {call.request.url}"
            if hasattr(call.request, "body") and call.request.body:
                requests_str += f"\n Body: {call.request.body}"

    def _assert_url_called(
        self,
        url: str,
        times: Optional[int] = None,
        method: Optional[str] = None,
    ):
        """Internal method to assert URL was called"""
        actual_calls = self.get_requests(
            url, None if method is None else method.upper()
        )

        if times is None:
            assert len(actual_calls), (
                f"Expected {url} {method} to be called, but it wasn't. Requests: \n{self.print_request_summary()}"
            )
        else:
            assert len(actual_calls) == times, (
                f"Expected {url}  {method} to be called {times} times, but it was called {actual_calls} times. Requests: \n{self.print_request_summary()}"
            )

    def _mock_404_response(self, url: str, mock_key: str):
        mock = self.responses.get(
            self.url_builder.datasets_url,
            status=404,
            headers=self.response_builder.create_headers(),
        )
        self._active_mocks[mock_key] = mock
        return mock

    @property
    def datasets_url(self) -> str:
        return self.url_builder.datasets_url

    @property
    def versions_url(self) -> str:
        return self.url_builder.versions_url
