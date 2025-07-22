from datetime import datetime
import json
from math import ceil
import re
from typing import Callable, Dict, List, Optional, Any, Tuple

from requests import PreparedRequest
from dpypelines.pipeline.messages.utils import get_mimetype
from tests.integration.constants import (
    dataset_api_url,
    upload_service_url,
    test_dataset_id,
    test_edition_id,
)
from responses import CallList, matchers, RequestsMock

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


def custom_matcher(
    params: Optional[Dict], *, strict_match: bool = True
) -> Callable[..., Any]:
    """
    Matcher to match 'params' argument keys in request.
    Based on responses.matchers.query_param_matcher Callable

    Parameters
    ----------
    params : dict
        The same as provided to request or a part of it if used in
        conjunction with ``strict_match=False``.
    strict_match : bool, default=True
        If set to ``True``, validates that all parameters match.
        If set to ``False``, original request may contain additional parameters.


    Returns
    -------
    Callable
        Matcher function.
    """
    params_dict = params or {}

    for k, v in params_dict.items():
        if isinstance(v, (int, float)):
            params_dict[k] = str(v)

    def match(request: PreparedRequest) -> Tuple[bool, str]:
        reason = ""
        request_params = request.params  # type:ignore
        request_params_dict = request_params or {}

        if not strict_match:
            # filter down to just the params specified in the matcher
            request_params_dict = {
                k: v for k, v in request_params_dict.items() if k in params_dict
            }
        valid = sorted(params_dict.keys()) == sorted(request_params_dict.keys())

        if not valid:
            reason = f"Keys do not match. {request_params_dict.keys()} doesn't match {params_dict.keys()}"
            if not strict_match:
                reason += (
                    "\nYou can use `strict_match=True` to do a strict parameters check."
                )
        return valid, reason

    return match


class ApiUrlBuilder:
    """Builds URLs for API endpoints"""

    def __init__(
        self,
        base_dataset_api_url: str = dataset_api_url,
        upload_service_url: str = upload_service_url,
        dataset_id: str = test_dataset_id,
        edition_id: str = test_edition_id,
    ):
        self.base_dataset_api_url = base_dataset_api_url
        self.upload_service_url = upload_service_url
        self.dataset_id = dataset_id
        self.edition_id = edition_id

    @property
    def datasets_url(self) -> str:
        return f"{self.base_dataset_api_url}/{self.dataset_id}"

    @property
    def versions_url(self) -> str:
        return f"{self.base_dataset_api_url}/{self.dataset_id}/editions/{self.edition_id}/versions"

    def custom_url(self, path: str) -> str:
        return f"{self.base_dataset_api_url}/{path}"

    def get_upload_params(self, filename: str, file_size: int, dataset_id: str) -> dict:
        mimetype = get_mimetype(f".{filename.split('.')[-1]}")
        chunks = ceil(file_size / 5242880)
        timestamp = datetime.now().strftime("%d%m%y%H%M%S")
        identifier = f"{timestamp}-{filename.replace('.', '-')}"
        return {
            "resumableFilename": filename,
            "resumableType": mimetype,
            "resumableTotalChunks": chunks,
            "resumableChunkSize": 5242880,
            "aliasName": filename,
            "resumableTotalSize": file_size,
            "resumableIdentifier": identifier,
            "resumableRelativePath": f"/tmp/{dataset_id}/{filename}",
            "LicenceUrl": "http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/",
            "isPublishable": "False",
            "Title": f"{filename.split('.')[0]}",
            "SizeInBytes": file_size,
            "Type": mimetype,
            "Licence": "Open Government Licence v3.0",
            "Path": f"datasets/{identifier}",
            "collectionId": "collection-id",
            "resumableChunkNumber": 1,
            "resumableCurrentChunkSize": file_size,
        }


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


class MockAPIResponses:
    """Main class for managing Dataset API and Upload Service mock responses"""

    def __init__(self, responses: RequestsMock):
        self.responses = responses
        self.api_url_builder = ApiUrlBuilder()
        self.dataset_api_response_builder = DatasetApiResponseBuilder()
        self._active_mocks = {}

        # Set up default mocks
        self.setup_defaults()

    def setup_defaults(
        self,
        mock_get_dataset: bool = True,
        mock_get_versions: bool = True,
        mock_post_versions: bool = True,
        mock_upload_service: bool = True,
    ):
        """Set up default mocks for common scenarios"""
        # Add passthru for Docker container
        self.responses.add_passthru(prefix=re.compile(pattern=r"http\+docker://"))
        self.remove_all_mocks()
        if mock_get_dataset:
            self.mock_get_dataset()
        if mock_get_versions:
            self.mock_get_versions()
        if mock_post_versions:
            self.mock_post_versions()
        if mock_upload_service:
            self.mock_upload_service()

    def mock_upload_service(
        self,
        filename: str = "data.csv",
        file_size: int = 0,
        dataset_id: str = "dataset_id",
        status_code: int = 201,
    ) -> Any:
        mock_key = "upload_service"
        self._remove_existing_mock(mock_key)

        if status_code == 404:
            return self._mock_404_response(
                self.api_url_builder.upload_service_url, mock_key, "post"
            )

        params = self.api_url_builder.get_upload_params(filename, file_size, dataset_id)

        mock_response = self.responses.post(
            self.api_url_builder.upload_service_url,
            body=b"",
            status=status_code,
            content_type="multipart/form-data",
            headers=self.dataset_api_response_builder.create_headers(),
            match=[custom_matcher(params=params, strict_match=False)],
        )

        self._active_mocks[mock_key] = mock_response
        return mock_response

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
            return self._mock_404_response(
                self.api_url_builder.datasets_url, mock_key, "get"
            )

        if custom_response:
            response_body = custom_response
        else:
            version_type = "static" if is_static else "filterable"
            response_body = self.dataset_api_response_builder.dataset_response(
                version_type, state
            )

        mock_response = self.responses.get(
            self.api_url_builder.datasets_url,
            body=json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.dataset_api_response_builder.create_headers(),
        )

        self._active_mocks[mock_key] = mock_response
        return mock_response

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
            return self._mock_404_response(
                self.api_url_builder.versions_url, mock_key, "get"
            )

        if custom_response:
            response_body = custom_response
        else:
            if items is None:
                items = [mock_versions[0]]
            response_body = self.dataset_api_response_builder.versions_response(items)

        mock_response = self.responses.get(
            self.api_url_builder.versions_url,
            body=json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.dataset_api_response_builder.create_headers(),
        )

        self._active_mocks[mock_key] = mock_response
        return mock_response

    def mock_post_versions(
        self,
        status_code: int = 201,
        response_body: Optional[Dict[str, Any]] = None,
        request_body: Optional[Dict[str, Any]] = None,
    ):
        mock_key = "post_versions"
        self._remove_existing_mock(mock_key)

        if status_code == 404:
            return self._mock_404_response(
                self.api_url_builder.versions_url, mock_key, "post"
            )

        response_matchers = (
            [] if request_body is None else [matchers.json_params_matcher(request_body)]
        )
        mock_response = self.responses.post(
            self.api_url_builder.versions_url,
            body=None if response_body is None else json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.dataset_api_response_builder.create_headers(),
            match=response_matchers,
        )

        self._active_mocks[mock_key] = mock_response
        return mock_response

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
        mock_response = method_func(
            url,
            body=json.dumps(response_body),
            status=status_code,
            content_type="application/json",
            headers=self.dataset_api_response_builder.create_headers(),
        )

        self._active_mocks[mock_key] = mock_response
        return mock_response

    def mock_dataset_error(
        self, status_code: int = 500, message: str = "Internal Server Error"
    ):
        """Mock dataset endpoint to return an error"""
        error_response = self.dataset_api_response_builder.error_response(message)
        return self.mock_get_dataset(
            custom_response=error_response, status_code=status_code
        )

    def mock_get_versions_error(
        self, status_code: int = 500, message: str = "Internal Server Error"
    ):
        """Mock versions endpoint to return an error"""
        error_response = self.dataset_api_response_builder.error_response(message)
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

    def assert_upload_service_called(self, times: Optional[int] = None):
        matches = self.get_requests_to_url_regex(
            re.compile(r"^http://test-upload-service.url/upload-new")
        )
        assert len(matches) == times

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
        self.assert_upload_service_called(times=1)

    def assert_all_dataset_api_requests_made(self):
        """Assert that all expected Dataset API requests were made"""
        self.assert_get_dataset_called(times=1)
        self.assert_get_versions_called(times=1)
        self.assert_post_versions_called(times=1)

    def assert_no_dataset_api_requests_made(self):
        self.assert_get_dataset_called(times=0)
        self.assert_get_versions_called(times=0)
        self.assert_post_versions_called(times=0)

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

    def get_all_requests(self) -> CallList:
        """Get all requests that were made"""
        return self.responses.calls

    def get_requests_to_url(self, url: str) -> List[Any]:
        """Get all requests made to a specific URL"""
        return [call for call in self.responses.calls if call.request.url == url]

    def get_requests_to_url_regex(self, url_pattern) -> List[Any]:
        return [
            call
            for call in self.responses.calls
            if re.match(url_pattern, call.request.url)  # type:ignore
        ]

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

    def _mock_404_response(self, url: str, mock_key: str, method: str):
        if method == "get":
            mock = self.responses.get(
                url,
                status=404,
                headers=self.dataset_api_response_builder.create_headers(),
            )
        elif method == "post":
            mock = self.responses.post(
                url,
                status=404,
                headers=self.dataset_api_response_builder.create_headers(),
            )
        self._active_mocks[mock_key] = mock  # type:ignore
        return mock  # type:ignore

    @property
    def datasets_url(self) -> str:
        return self.api_url_builder.datasets_url

    @property
    def versions_url(self) -> str:
        return self.api_url_builder.versions_url
