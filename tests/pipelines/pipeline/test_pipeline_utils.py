import json
import os
from unittest.mock import MagicMock, patch

from dpypelines.pipeline.utils import (
    send_submission_confirmation,
    setup_clients,
    upload_files,
    upload_metadata,
)


@patch("dpypelines.pipeline.utils.get_notifier")
@patch("dpypelines.pipeline.utils.get_email_client")
def test_setup_clients(mock_get_email_client, mock_get_notifier):
    """Test that `setup_clients()` returns the expected clients."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_get_notifier.return_value = mock_notifier
    mock_get_email_client.return_value = mock_email_client

    notifier, email_client = setup_clients()

    assert notifier == mock_notifier
    assert email_client == mock_email_client


@patch("dpypelines.pipeline.utils.JobConfiguration")
@patch("dpypelines.pipeline.utils.UploadServiceClient")
@patch("dpypelines.pipeline.utils.get_mimetype")
def test_upload_files(mock_get_mimetype, mock_UploadServiceClient, mock_job_config):
    """Test that `upload_files()` uploads the files and sends the email."""
    mock_validation_results = {"config_files": ["file1", "file2"]}
    mock_upload_client = MagicMock()
    mock_job_configuration = MagicMock()

    upload_url = "http://upload.url"
    dataset_api_url = "http://datasetapi.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url

    mock_get_mimetype.return_value = "text/csv"
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_job_config.return_value = mock_job_configuration

    upload_files(mock_validation_results)

    mock_UploadServiceClient.assert_called_once_with(upload_url)
    mock_get_mimetype.assert_called()
    mock_upload_client.upload_new.assert_called()


@patch("dpypelines.pipeline.utils.submission_processed_email")
def test_send_submission_confirmation(mock_submission_processed_email):
    """Test that `send_submission_confirmation()` sends the submission confirmation email."""
    mock_email_client = MagicMock()
    mock_submitter_email = "test@example.com"
    mock_email_content = MagicMock()

    mock_submission_processed_email.return_value = mock_email_content

    send_submission_confirmation(mock_email_client, mock_submitter_email)

    mock_email_client.send.assert_called_once_with(
        mock_submitter_email, mock_email_content.subject, mock_email_content.message
    )


@patch("dpypelines.pipeline.utils.JobConfiguration")
@patch("dpypelines.pipeline.utils.check_dataset_type_is_static")
@patch("dpypelines.pipeline.utils.get_post_request_values_from_metadata")
@patch("dpypelines.pipeline.utils.DatasetAPIClient")
def test_upload_metadata_succeeds(
    mock_DatasetAPIClient, mock_request_values, mock_dataset_type, mock_job_config
):
    mock_dataset_api_client = MagicMock()
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.get_path.return_value.status_code = 200
    mock_dataset_api_client.post_json.return_value.status_code = 201

    mock_job_configuration = MagicMock()

    upload_url = "http://upload.url"
    dataset_api_url = "http://dataset-api.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url

    mock_request_values.return_value = (
        "dataset_path",
        "edition_path",
        {"key": "value"},
    )
    mock_dataset_type.return_value = True
    with open("tests/fixtures/test-cases/test_metadata.json") as f:
        metadata = json.load(f)
    mock_job_config.return_value = mock_job_configuration

    metadata_uploaded = upload_metadata(metadata)

    mock_DatasetAPIClient.assert_called_once_with(
        "http://dataset-api.url", "dataset_path", "edition_path"
    )
    mock_dataset_api_client.get_path.assert_called_once()

    mock_request_values.assert_called_once_with(metadata)
    mock_dataset_type.assert_called_once_with(mock_dataset_api_client)
    assert metadata_uploaded


@patch("dpypelines.pipeline.utils.JobConfiguration")
@patch("dpypelines.pipeline.utils.check_dataset_type_is_static")
@patch("dpypelines.pipeline.utils.get_post_request_values_from_metadata")
@patch("dpypelines.pipeline.utils.DatasetAPIClient")
def test_upload_metadata_fails_not_static(
    mock_DatasetAPIClient, mock_request_values, mock_dataset_type, mock_job_config
):
    mock_dataset_api_client = MagicMock()
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.get_path.return_value.status_code = 200

    mock_job_configuration = MagicMock()

    upload_url = "http://upload.url"
    dataset_api_url = "http://dataset-api.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url

    mock_request_values.return_value = (
        "dataset_path",
        "edition_path",
        {"key": "value"},
    )
    mock_dataset_type.return_value = False
    with open("tests/fixtures/test-cases/test_metadata.json") as f:
        metadata = json.load(f)

    mock_job_config.return_value = mock_job_configuration

    metadata_uploaded = upload_metadata(metadata)

    mock_DatasetAPIClient.assert_called_once_with(
        "http://dataset-api.url", "dataset_path", "edition_path"
    )

    mock_request_values.assert_called_once_with(metadata)
    mock_dataset_type.assert_called_once_with(mock_dataset_api_client)
    assert not metadata_uploaded


@patch("dpypelines.pipeline.utils.JobConfiguration")
@patch("dpypelines.pipeline.utils.check_dataset_type_is_static")
@patch("dpypelines.pipeline.utils.get_post_request_values_from_metadata")
@patch("dpypelines.pipeline.utils.DatasetAPIClient")
def test_upload_metadata_fails_get_path_404(
    mock_DatasetAPIClient, mock_request_values, mock_dataset_type, mock_job_config
):
    mock_dataset_api_client = MagicMock()
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.get_path.return_value.status_code = 404

    mock_job_configuration = MagicMock()

    upload_url = "http://upload.url"
    dataset_api_url = "http://dataset-api.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url

    mock_request_values.return_value = (
        "dataset_path",
        "edition_path",
        {"key": "value"},
    )
    mock_dataset_type.return_value = True
    with open("tests/fixtures/test-cases/test_metadata.json") as f:
        metadata = json.load(f)

    mock_job_config.return_value = mock_job_configuration

    upload_metadata(metadata)

    mock_DatasetAPIClient.assert_called_once_with(
        "http://dataset-api.url", "dataset_path", "edition_path"
    )
    mock_dataset_api_client.get_path.return_value.raise_for_status.assert_called_once()
    mock_request_values.assert_called_once_with(metadata)
    mock_dataset_type.assert_called_once_with(mock_dataset_api_client)
