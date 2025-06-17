from pathlib import Path
from unittest.mock import MagicMock, call

from dpypelines.pipeline.api.upload import upload_files
from dpypelines.pipeline.models.metadata_models import Distribution


def test_upload_files():
    """Test that `upload_files()` uploads the files and sends the email."""
    mock_validation_results = [
        Distribution(title="distribution 1", file="example-file.csv", format="csv"),
        Distribution(title="distribution 2", file="other-file.csv", format="csv"),
    ]
    mock_upload_client = MagicMock()
    mock_job_configuration = MagicMock()
    upload_url = "http://upload.url"
    dataset_api_url = "http://datasetapi.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url

    decompressed_file_dir = Path("example/")
    upload_files(
        decompressed_file_dir,
        mock_validation_results,
        mock_job_configuration,
        mock_upload_client,
    )
    expected_upload_client_calls = [
        call(
            file_path=decompressed_file_dir / distribution.file,
            mimetype=distribution.media_type,
            upload_path=distribution.upload_path,
            identifier=distribution.identifier,
        )
        for distribution in mock_validation_results
    ]
    mock_upload_client.upload_new.assert_has_calls(expected_upload_client_calls)
    mock_upload_client.upload_new.assert_called()
