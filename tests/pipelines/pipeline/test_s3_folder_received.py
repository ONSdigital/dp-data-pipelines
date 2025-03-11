import os
import io
from zipfile import ZipFile
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.utils import (
    decompress_file,
    send_submission_confirmation,
    setup_clients,
    upload_files,
    validate_pipeline,
)
from dpypelines.pipeline.validate_pipeline import retrieve_config_and_files
from dpypelines.s3_folder_received import start


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


@patch("dpypelines.pipeline.utils._get_s3_client")
@patch("dpypelines.pipeline.utils.LocalDirectoryStore")
@patch("dpypelines.pipeline.utils.Path.exists", return_value=True)
def test_decompress_file(mock_path_exists, mock_LocalDirectoryStore, mock_get_s3_client):
    """Test that `decompress_file()` returns the expected local store."""
    # Set up a fake S3 client
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # Create an in-memory zip file containing "test_file.txt"
    fake_zip = io.BytesIO()
    with ZipFile(fake_zip, 'w') as zf:
        zf.writestr("test_file.txt", "Test Zip file")
    fake_zip.seek(0)

    # When download_fileobj is called, write the fake zip content into the provided file-like object
    mock_s3.download_fileobj.side_effect = lambda bucket, key, f: f.write(fake_zip.getvalue())

    # Set up a mock local directory store that will be returned by decompress_file
    mock_local_store = MagicMock()
    mock_LocalDirectoryStore.return_value = mock_local_store
    mock_local_store.get_file_names.return_value = ["test_file"]

    try:
        local_store = decompress_file("s3_object_name")
        assert local_store == mock_local_store
    finally:
        # Cleanup: delete test_file.txt if it exists in the current working directory
        test_file_path = Path("input/test_file.txt")
        if test_file_path.exists():
            os.remove(test_file_path)


@patch("dpypelines.pipeline.validate_pipeline.retrieve_manifest")
@patch("dpypelines.pipeline.validate_pipeline.get_source_id_from_manifest")
@patch("dpypelines.pipeline.validate_pipeline.get_pipeline_config_for_source")
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


@patch("dpypelines.pipeline.utils.validate_pipeline_files")
def test_validate_pipeline(mock_validate_pipeline_files):
    """Test that `validate_pipeline()` returns the expected validation results."""
    mock_files_dir = "files_dir"
    mock_pipeline_config = {"config": "value"}
    mock_validation_results = {"manifest": "value"}

    mock_validate_pipeline_files.return_value = mock_validation_results

    validation_results = validate_pipeline(mock_files_dir, mock_pipeline_config)

    assert validation_results == mock_validation_results


@patch("dpypelines.pipeline.utils.UploadServiceClient")
@patch("dpypelines.pipeline.utils.get_mimetype")
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


@patch(
    "dpypelines.s3_folder_received.decompress_file",
    side_effect=FileNotFoundError(
        "[Errno 2] No such file or directory: 'dummy_s3_object_name'"
    ),
)
@patch("dpypelines.pipeline.utils.Path.is_dir", return_value=True)
@patch("dpypelines.pipeline.utils.Path.exists", return_value=True)
@patch("dpypelines.pipeline.utils.setup_clients")
@patch("dpypelines.pipeline.validate_pipeline.retrieve_config_and_files")
@patch("dpypelines.pipeline.utils.validate_pipeline")
@patch("dpypelines.pipeline.utils.upload_files")
@patch("dpypelines.pipeline.utils.send_submission_confirmation")
def test_start_valid_data(
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_decompress_file,
    mock_setup_clients,
    mock_path_exists,
    mock_path_isdir,
):
    """Test that `start()` raises FileNotFoundError as expected."""
    mock_setup_clients.return_value = (MagicMock(), MagicMock())
    mock_retrieve_config_and_files.return_value = (
        {"fileAuthorEmail": "test@example.com"},
        {"config": "value"},
        "files_dir",
    )
    mock_validate_pipeline.side_effect = FileNotFoundError(
        "[Errno 2] No such file or directory: 'dummy_s3_object_name'"
    )

    with pytest.raises(
        Exception,
        match=re.escape("[Errno 2] No such file or directory: 'dummy_s3_object_name'"),
    ):
        start("dummy_s3_object_name")


@patch("dpypelines.pipeline.utils.Path.is_dir", return_value=True)
@patch("dpypelines.pipeline.utils.Path.exists", return_value=True)
@patch("dpypelines.pipeline.utils.setup_clients")
@patch("dpypelines.pipeline.utils.decompress_file")
@patch("dpypelines.pipeline.validate_pipeline.retrieve_config_and_files")
@patch("dpypelines.pipeline.utils.validate_pipeline")
@patch("dpypelines.pipeline.utils.upload_files")
@patch("dpypelines.pipeline.utils.send_submission_confirmation")
def test_start_missing_files(
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_decompress_file,
    mock_setup_clients,
    mock_path_exists,
    mock_path_isdir,
):
    """Test that `start()` raises an exception when the manifest is invalid."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock()
    mock_decompress_file.return_value = mock_local_store
    mock_manifest_dict = {"fileAuthorEmail": "test@example.com"}
    mock_pipeline_config = {"config": "value"}
    mock_files_dir = "files_dir"
    mock_retrieve_config_and_files.return_value = (
        mock_manifest_dict,
        mock_pipeline_config,
        mock_files_dir,
    )
    mock_validate_pipeline.side_effect = ValueError("Invalid manifest file")
