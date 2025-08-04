from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, call
from uuid import uuid4

from dpypelines.pipeline.api.upload import upload_files


@patch("dpypelines.pipeline.api.upload.datetime")
@patch("dpypelines.pipeline.api.upload.uuid4")
@patch("dpypelines.pipeline.api.upload.get_mimetype")
def test_upload_files(mock_get_mimetype, mock_uuid, mock_timestamp):
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

    mock_uuid.return_value = uuid4()
    mock_timestamp.now.return_value.strftime.return_value = datetime.now().strftime(
        "%d-%m-%yT%H-%M-%S"
    )

    upload_files(mock_validation_results, mock_job_configuration, mock_upload_client)

    expected_upload_client_calls = [
        call(
            file_path=path,
            mimetype=mock_mimetype,
            upload_path=f"datasets/{mock_timestamp.now.return_value.strftime.return_value}-{mock_uuid.return_value}-{path}",
            identifier=f"{mock_timestamp.now.return_value.strftime.return_value}-{mock_uuid.return_value}-{path}",
        )
        for path in mock_validation_results
    ]
    mock_get_mimetype.assert_called()

    for expected_call in expected_upload_client_calls:
        assert expected_call in mock_upload_client.upload_new.mock_calls
