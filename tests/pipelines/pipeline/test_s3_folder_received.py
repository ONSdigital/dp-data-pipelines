import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.s3_folder_received import (
    decompress_tar_file,
    retrieve_config_and_files,
    send_submission_confirmation,
    setup_clients,
    start,
    upload_files,
    validate_pipeline,
)


@patch("dpypelines.s3_folder_received.get_notifier")
@patch("dpypelines.s3_folder_received.get_email_client")
def test_setup_clients(mock_get_email_client, mock_get_notifier):
    """Test that `setup_clients()` returns the expected clients."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_get_notifier.return_value = mock_notifier
    mock_get_email_client.return_value = mock_email_client

    notifier, email_client = setup_clients()

    assert notifier == mock_notifier
    assert email_client == mock_email_client


@patch("dpypelines.s3_folder_received.LocalDirectoryStore")
def test_decompress_tar_file(mock_LocalDirectoryStore):
    """Test that `decompress_tar_file()` returns the expected local store."""
    mock_local_store = MagicMock()
    mock_LocalDirectoryStore.return_value = mock_local_store
    mock_local_store.get_file_names.return_value = ["file1", "file2"]

    local_store = decompress_tar_file("s3_object_name")

    assert local_store == mock_local_store
    mock_LocalDirectoryStore.assert_called_once_with("s3_object_name")


@patch("dpypelines.s3_folder_received.retrieve_manifest")
@patch("dpypelines.s3_folder_received.get_source_id_from_manifest")
@patch("dpypelines.s3_folder_received.get_pipeline_config_for_source")
def test_retrieve_config_and_files(
    mock_get_pipeline_config_for_source,
    mock_get_source_id_from_manifest,
    mock_retrieve_manifest,
):
    """Test that `retrieve_config_and_files_dir()` returns the expected manifest, pipeline_Config  and files dir."""
    mock_local_store = MagicMock()
    mock_manifest_dict = {"key": "value"}
    mock_source_id = "source_id"
    mock_pipeline_config = {"config": "value"}
    mock_files_dir = Path("files_dir")

    mock_retrieve_manifest.return_value = mock_manifest_dict
    mock_get_source_id_from_manifest.return_value = mock_source_id
    mock_get_pipeline_config_for_source.return_value = mock_pipeline_config
    mock_local_store.get_current_source_pathlike.return_value = mock_files_dir

    manifest_dict, pipeline_config, files_dir = retrieve_config_and_files(
        mock_local_store
    )

    assert manifest_dict == mock_manifest_dict
    assert pipeline_config == mock_pipeline_config
    assert files_dir == mock_files_dir


@patch("dpypelines.s3_folder_received.validate_pipeline_files")
def test_validate_pipeline(mock_validate_pipeline_files):
    """Test that `validate_pipeline()` returns the expected validation results."""
    mock_files_dir = "files_dir"
    mock_pipeline_config = {"config": "value"}
    mock_validation_results = {"manifest": "value"}

    mock_validate_pipeline_files.return_value = mock_validation_results

    validation_results = validate_pipeline(mock_files_dir, mock_pipeline_config)

    assert validation_results == mock_validation_results


@patch("dpypelines.s3_folder_received.UploadServiceClient")
@patch("dpypelines.s3_folder_received.get_mimetype")
def test_upload_files(mock_get_mimetype, mock_UploadServiceClient):
    """Test that `upload_files()` uploads the files and sends the email."""
    mock_validation_results = {"config_files": ["file1", "file2"]}
    mock_email_client = MagicMock()
    mock_submitter_email = "test@example.com"
    mock_upload_client = MagicMock()

    mock_get_mimetype.return_value = "text/csv"
    mock_UploadServiceClient.return_value = mock_upload_client

    with patch.dict(
        os.environ,
        {
            "UPLOAD_SERVICE_URL": "http://upload.url",
            "DATASET_API_URL": "http://dataset.api.url",
        },
    ):
        upload_files(mock_validation_results, mock_email_client, mock_submitter_email)

    mock_UploadServiceClient.assert_called_once_with("http://upload.url")
    mock_get_mimetype.assert_called()
    mock_upload_client.upload_new.assert_called()
    mock_email_client.send.assert_called()


@patch("dpypelines.s3_folder_received.submission_processed_email")
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


@patch("dpypelines.s3_folder_received.setup_clients")
@patch("dpypelines.s3_folder_received.decompress_tar_file")
@patch("dpypelines.s3_folder_received.retrieve_config_and_files")
@patch("dpypelines.s3_folder_received.validate_pipeline")
@patch("dpypelines.s3_folder_received.upload_files")
@patch("dpypelines.s3_folder_received.send_submission_confirmation")
def test_start_valid_data(
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_decompress_tar_file,
    mock_setup_clients,
):
    """Test that `start()` returns True when the pipeline is successfully processed."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock()
    mock_decompress_tar_file.return_value = mock_local_store
    mock_manifest_dict = {"fileAuthorEmail": "test@example.com"}
    mock_pipeline_config = {"config": "value"}
    mock_files_dir = "files_dir"
    mock_retrieve_config_and_files.return_value = (
        mock_manifest_dict,
        mock_pipeline_config,
        mock_files_dir,
    )
    mock_validation_results = {"manifest": "value"}
    mock_validate_pipeline.return_value = mock_validation_results

    result = start("dummy_s3_object_name")

    assert result is True
    mock_setup_clients.assert_called_once()
    mock_decompress_tar_file.assert_called_once_with("dummy_s3_object_name")
    mock_retrieve_config_and_files.assert_called_once_with(mock_local_store)
    mock_validate_pipeline.assert_called_once_with(mock_files_dir, mock_pipeline_config)
    mock_upload_files.assert_called_once_with(
        mock_validation_results, mock_email_client, "test@example.com"
    )
    mock_send_submission_confirmation.assert_called_once_with(
        mock_email_client, "test@example.com"
    )
    mock_notifier.success.assert_called_once()


@patch("dpypelines.s3_folder_received.setup_clients")
@patch("dpypelines.s3_folder_received.decompress_tar_file")
@patch("dpypelines.s3_folder_received.retrieve_config_and_files")
@patch("dpypelines.s3_folder_received.validate_pipeline")
@patch("dpypelines.s3_folder_received.upload_files")
@patch("dpypelines.s3_folder_received.send_submission_confirmation")
def test_start_missing_files(
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_decompress_tar_file,
    mock_setup_clients,
):
    """Test that `start()` raises an exception when required files are missing."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock()
    mock_decompress_tar_file.return_value = mock_local_store
    mock_manifest_dict = {"fileAuthorEmail": "test@example.com"}
    mock_pipeline_config = {"config": "value"}
    mock_files_dir = "files_dir"
    mock_retrieve_config_and_files.return_value = (
        mock_manifest_dict,
        mock_pipeline_config,
        mock_files_dir,
    )
    mock_validate_pipeline.side_effect = FileNotFoundError("Required file not found")

    with pytest.raises(Exception, match="Required file not found"):
        start("dummy_s3_object_name")


@patch("dpypelines.s3_folder_received.setup_clients")
@patch("dpypelines.s3_folder_received.decompress_tar_file")
@patch("dpypelines.s3_folder_received.retrieve_config_and_files")
@patch("dpypelines.s3_folder_received.validate_pipeline")
@patch("dpypelines.s3_folder_received.upload_files")
@patch("dpypelines.s3_folder_received.send_submission_confirmation")
def test_start_invalid_manifest(
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_decompress_tar_file,
    mock_setup_clients,
):
    """Test that `start()` raises an exception when the manifest is invalid."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock()
    mock_decompress_tar_file.return_value = mock_local_store
    mock_manifest_dict = {"fileAuthorEmail": "test@example.com"}
    mock_pipeline_config = {"config": "value"}
    mock_files_dir = "files_dir"
    mock_retrieve_config_and_files.return_value = (
        mock_manifest_dict,
        mock_pipeline_config,
        mock_files_dir,
    )
    mock_validate_pipeline.side_effect = ValueError("Invalid manifest file")

    with pytest.raises(Exception, match="Invalid manifest file"):
        start("dummy_s3_object_name")
