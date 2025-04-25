from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.models import (
    Alert,
    Distribution,
    Manifest,
    Metadata,
    SubmissionContact,
    UsageNote,
)
from dpypelines.s3_folder_received import start


@patch("dpypelines.s3_folder_received.JobConfiguration")
@patch("dpypelines.s3_folder_received.delete_s3_processing_folder")
@patch("dpypelines.s3_folder_received.copy_s3_processing_folder_to_destination_folder")
@patch("dpypelines.s3_folder_received.validate_and_upload_metadata")
@patch("dpypelines.s3_folder_received.upload_files")
@patch("dpypelines.s3_folder_received.validate_pipeline_files")
@patch("dpypelines.s3_folder_received.validate_manifest")
@patch("dpypelines.s3_folder_received.process_zip_file")
@patch("dpypelines.s3_folder_received.setup_clients")
def test_start_succeeds(
    mock_setup_clients,
    mock_process_zip_file,
    mock_manifest_validation,
    mock_pipeline_validation,
    mock_upload_files,
    mock_upload_metadata,
    mock_copy_s3_processing,
    mock_delete_s3_processing,
    mock_job_config,
):
    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock(name="local_store")
    mock_process_zip_file.return_value = (
        mock_local_store,
        Path("files_dir"),
        "processing/timestamp-files",
    )
    mock_manifest_validation.return_value = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
    )
    mock_pipeline_validation.return_value = Metadata(
        dataset_id="dataset-id",
        edition="edition-id",
        edition_title="Edition title",
        quality_designation="quality-designation",
        release_date="2025-01-01T00:00:00.000Z",
        usage_notes=[UsageNote(title="Usage note title", note="Usage note content")],
        alerts=[Alert(type="alert", description="Alert description")],
        distributions=[
            Distribution(
                title="Distribution title", format="csv", file="distribution.csv"
            )
        ],
    )
    mock_upload_metadata.return_value = True
    mock_copy_s3_processing.return_value = "processed/timestamp-files"
    mock_job_configuration = MagicMock()

    upload_url = "http://upload.url"
    dataset_api_url = "http://datasetapi.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url
    mock_job_configuration.skip_data_upload = False
    mock_job_config.return_value = mock_job_configuration

    start("dummy_s3_object_name")

    mock_setup_clients.assert_called_once()
    mock_process_zip_file.assert_called_once_with("dummy_s3_object_name")
    mock_manifest_validation.assert_called_once_with(mock_local_store)
    mock_pipeline_validation.assert_called_once_with(
        mock_manifest_validation.return_value, mock_local_store
    )
    mock_upload_metadata.assert_called_once_with(mock_pipeline_validation.return_value)
    mock_upload_files.assert_called_once_with([Path("files_dir") / "distribution.csv"])
    mock_copy_s3_processing.assert_called_once_with(
        "dummy_s3_object_name",
        Path("files_dir"),
        "processing/timestamp-files",
        "processed",
    )
    mock_notifier.success.assert_called_once()
    mock_delete_s3_processing.assert_called_once_with(
        "dummy_s3_object_name", Path("files_dir"), "processing/timestamp-files"
    )


@patch("dpypelines.s3_folder_received.JobConfiguration")
@patch("dpypelines.s3_folder_received.delete_s3_processing_folder")
@patch("dpypelines.s3_folder_received.copy_s3_processing_folder_to_destination_folder")
@patch("dpypelines.s3_folder_received.validate_and_upload_metadata")
@patch("dpypelines.s3_folder_received.validate_pipeline_files")
@patch("dpypelines.s3_folder_received.validate_manifest")
@patch("dpypelines.s3_folder_received.process_zip_file")
@patch("dpypelines.s3_folder_received.setup_clients")
def test_start_fails_dataset_not_static(
    mock_setup_clients,
    mock_process_zip_file,
    mock_manifest_validation,
    mock_pipeline_validation,
    mock_upload_metadata,
    mock_copy_s3_processing,
    mock_delete_s3_processing,
    mock_job_config,
):
    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock(name="local_store")
    mock_process_zip_file.return_value = (
        mock_local_store,
        Path("files_dir"),
        "processing/timestamp-files",
    )
    mock_manifest_validation.return_value = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
    )
    mock_pipeline_validation.return_value = Metadata(
        dataset_id="dataset-id",
        edition="edition-id",
        edition_title="Edition title",
        quality_designation="quality-designation",
        release_date="2025-01-01T00:00:00.000Z",
        usage_notes=[UsageNote(title="Usage note title", note="Usage note content")],
        alerts=[Alert(type="alert", description="Alert description")],
        distributions=[
            Distribution(
                title="Distribution title", format="csv", file="distribution.csv"
            )
        ],
    )
    mock_upload_metadata.return_value = False
    mock_copy_s3_processing.return_value = "processed/timestamp-files"

    mock_job_configuration = MagicMock()

    upload_url = "http://upload.url"
    dataset_api_url = "http://datasetapi.url"
    mock_job_configuration.upload_service_url = upload_url
    mock_job_configuration.dataset_api_url = dataset_api_url
    mock_job_configuration.skip_data_upload = False
    mock_job_config.return_value = mock_job_configuration

    start("dummy_s3_object_name")

    mock_setup_clients.assert_called_once()
    mock_process_zip_file.assert_called_once_with("dummy_s3_object_name")
    mock_manifest_validation.assert_called_once_with(mock_local_store)
    mock_pipeline_validation.assert_called_once_with(
        mock_manifest_validation.return_value, mock_local_store
    )
    mock_upload_metadata.assert_called_once_with(mock_pipeline_validation.return_value)
    mock_copy_s3_processing.assert_called_once_with(
        "dummy_s3_object_name",
        Path("files_dir"),
        "processing/timestamp-files",
        "dataset-type-not-static",
    )
    mock_delete_s3_processing.assert_called_once_with(
        "dummy_s3_object_name", Path("files_dir"), "processing/timestamp-files"
    )


@patch("dpypelines.s3_folder_received.error_handler")
@patch("dpypelines.s3_folder_received.validate_manifest")
@patch("dpypelines.s3_folder_received.process_zip_file")
@patch("dpypelines.s3_folder_received.setup_clients")
def test_start_fails_invalid_manifest(
    mock_setup_clients,
    mock_process_zip_file,
    mock_manifest_validation,
    mock_error_handler,
):
    """
    Test that `start()` raises an exception when the manifest is invalid.
    """
    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock(name="local_store")
    mock_process_zip_file.return_value = (
        mock_local_store,
        Path("decompressed_files_dir"),
        "processing/timestamp-file",
    )
    mock_manifest_validation.side_effect = FileNotFoundError(
        "Failed to retrieve manifest from the local directory store."
    )
    mock_delete_s3_processing = MagicMock("delete_s3_processing")
    mock_delete_s3_processing.return_value = None
    mock_error_handler = MagicMock("error_handler")
    mock_error_handler.return_value = None
    with pytest.raises(FileNotFoundError) as e:
        start("bucket/folder/file.zip")
    assert "Failed to retrieve manifest from the local directory store." in str(e)
    mock_notifier.failure.assert_called_once()
    mock_process_zip_file.assert_called_once_with("bucket/folder/file.zip")
    mock_manifest_validation.assert_called_once_with(mock_local_store)
