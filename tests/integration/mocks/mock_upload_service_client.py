from datetime import datetime
import json
from math import ceil
from typing import Any, Dict, Optional
from dpypelines.pipeline.messages.utils import get_mimetype
from tests.integration.constants import upload_service_url


class UploadServiceUrlBuilder:
    """Builds URLs for Upload Service endpoints"""

    def __init__(
        self,
        base_url: str = upload_service_url,
    ):
        self.base_url = base_url

    # TODO sort out @property once generate_path_url is done
    # @property
    # def upload_url(self) -> str:
    #     return f"{self.base_url}{self.path_url}"


class UploadServiceResponseBuilder:
    @staticmethod
    def upload_response():
        return b""

    @staticmethod
    def error_response(description: str, code: str = "ERROR") -> Dict[str, Any]:
        return {"error": {"description": description, "code": code}}

    @staticmethod
    def create_headers() -> Dict[str, str]:
        return {"Date": datetime.now().strftime("%a, %d %b %Y %H:%M:%S GMT")}


class MockUploadService:
    def __init__(self, responses):
        self.responses = responses
        self.url_builder = UploadServiceUrlBuilder()
        self.response_builder = UploadServiceResponseBuilder()
        self._active_mocks = {}

        # Set up default mocks
        self.setup_defaults()

    # TODO sort out @property once generate_path_url is done
    # @property
    # def upload_url(self) -> str:
    #     return self.url_builder.upload_url

    def setup_defaults(self, mock_post_data: bool = True):
        self.remove_all_mocks()
        if mock_post_data:
            self.mock_post_data(filename="", file_size=0)

    # TODO Get file_size from file
    def generate_path_url(self, filename: str, file_size: int):
        self.filename = filename
        self.mimetype = get_mimetype(f".{filename.split('.')[-1]}")
        self.timestamp = datetime.now().strftime("%d%m%y%H%M%S")
        self.identifier = f"{self.timestamp}-{self.filename.replace('.', '-')}"
        self.chunks = ceil(file_size / 5242880)

        path_url = f"/upload-new?resumableFilename={self.filename}&resumableType={self.mimetype}&resumableTotalChunks={self.chunks}&resumableChunkSize=5242880&aliasName={self.filename}&resumableTotalSize={file_size}&resumableIdentifier={self.identifier}&resumableRelativePath=/tmp/{self.filename}&LicenceUrl=http%3A%2F%2Fwww.nationalarchives.gov.uk%2Fdoc%2Fopen-government-licence%2Fversion%2F3%2F&isPublishable=False&Title={self.filename.split('.')[0]}&SizeInBytes={file_size}&Type={self.mimetype}&Licence=Open+Government+Licence+v3.0&Path=datasets/{self.identifier}&collectionId=collection-id&resumableChunkNumber={self.chunks}&resumableCurrentChunkSize={file_size}"

        upload_url = f"{self.url_builder.base_url}{path_url}"
        return upload_url

    def mock_post_data(
        self,
        filename: str,
        file_size: int,
        status_code: int = 201,
        response_body: Optional[str] = None,
        request_body: Optional[str] = None,
    ):
        mock_key = "post_data"
        self._remove_existing_mock(mock_key)
        upload_url = self.generate_path_url(filename, file_size)
        if status_code == 404:
            return self._mock_404_response(upload_url, mock_key)

        # TODO use multipart_matcher or query_param_matcher here?
        # response_matchers = (
        #     [] if request_body is None else [matchers.json_params_matcher(request_body)]
        # )

        mock = self.responses.post(
            upload_url,
            status=status_code,
            content_type="multipart/form-data",
            headers=self.response_builder.create_headers(),
            body=None if response_body is None else json.dumps(response_body),
            # TODO populate files (or data) from file content
            # files="",
            # match=response_matchers,
        )

        self._active_mocks[mock_key] = mock
        return mock

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

    def _mock_404_response(self, url: str, mock_key: str):
        mock = self.responses.get(
            url,
            status=404,
            headers=self.response_builder.create_headers(),
        )
        self._active_mocks[mock_key] = mock
        return mock
