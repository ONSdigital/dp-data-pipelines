from pathlib import Path
from unittest.mock import MagicMock, patch, call

from dpypelines.pipeline.api.upload import upload_files


@patch("dpypelines.pipeline.api.upload.get_mimetype")
def test_upload_files(mock_get_mimetype):
    """Test that `upload_files()` uploads the files and sends the email."""
    mock_validation_results = [Path("file1"), Path("file2")]
    mock_upload_client = MagicMock()
    mock_job_configuration = MagicMock()
    upload_url = "http://upload.url"
    dataset_api_url = "http://datasetapi.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url

    mock_mimetype = "text/csv"
    mock_get_mimetype.return_value = mock_mimetype

    upload_files(mock_validation_results, mock_job_configuration, mock_upload_client)

    expected_upload_client_calls = [
        call(path, mock_mimetype) for path in mock_validation_results
    ]
    mock_upload_client.upload_new.assert_has_calls(expected_upload_client_calls)
    mock_get_mimetype.assert_called()
    mock_upload_client.upload_new.assert_called()
