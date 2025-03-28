from datetime import datetime
import io
import os
import re
from pathlib import Path
import tempfile
from unittest.mock import MagicMock, call, patch
from zipfile import ZipFile

import pytest

from dpypelines.pipeline.utils import (
    copy_s3_processing_folder_to_processed_folder,
    decompress_zip_file,
    delete_s3_processing_folder,
    download_zip_file,
    process_zip_file,
    send_submission_confirmation,
    setup_clients,
    upload_files,
    upload_to_s3_processing_folder,
    validate_pipeline,
)
from dpypelines.pipeline.validate_pipeline import retrieve_config_and_files
from dpypelines.s3_folder_received import start
from tests.pipelines.pipeline.mocks import (
    mock_path_constructor,
    mock_decompress_zip_file,
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
    "dpypelines.s3_folder_received.process_zip_file",
    side_effect=FileNotFoundError(
        "[Errno 2] No such file or directory: 'dummy_s3_object_name'"
    ),
)
@patch("dpypelines.pipeline.utils.Path.is_dir", return_value=True)
@patch("dpypelines.pipeline.utils.Path.exists", return_value=True)
@patch("dpypelines.s3_folder_received.setup_clients")
@patch("dpypelines.s3_folder_received.retrieve_config_and_files")
@patch("dpypelines.s3_folder_received.validate_pipeline")
@patch("dpypelines.s3_folder_received.upload_files")
@patch("dpypelines.s3_folder_received.send_submission_confirmation")
def test_start_valid_data(
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_setup_clients,
    mock_path_exists,
    mock_path_isdir,
    mock_process_zip_file,
):
    """
    Test that `start()` raises an Exception with the expected message when processing the zip file fails.
    """
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


@patch("dpypelines.s3_folder_received.process_zip_file")
@patch("dpypelines.pipeline.utils.Path.is_dir", return_value=True)
@patch("dpypelines.pipeline.utils.Path.exists", return_value=True)
@patch("dpypelines.s3_folder_received.setup_clients")
@patch("dpypelines.s3_folder_received.retrieve_config_and_files")
@patch("dpypelines.s3_folder_received.validate_pipeline")
@patch("dpypelines.s3_folder_received.upload_files")
@patch("dpypelines.s3_folder_received.send_submission_confirmation")
@patch("dpypelines.s3_folder_received.error_handler")
def test_start_missing_files(
    mock_error_handler,
    mock_send_submission_confirmation,
    mock_upload_files,
    mock_validate_pipeline,
    mock_retrieve_config_and_files,
    mock_setup_clients,
    mock_path_exists,
    mock_path_isdir,
    mock_process_zip_file,
):
    """
    Test that `start()` raises an exception when the manifest is invalid.
    """
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)
    mock_local_store = MagicMock()
    mock_decompressed_file_dir = Path("decompressed_files_dir")
    mock_s3_processing_folder = "processing"
    mock_process_zip_file.return_value = (
        mock_local_store,
        mock_decompressed_file_dir,
        mock_s3_processing_folder,
    )
    mock_manifest_dict = {"fileAuthorEmail": "test@example.com"}
    mock_pipeline_config = {"config": "value"}
    mock_files_dir = "files_dir"
    mock_local_store.get_current_source_pathlike.return_value = Path(mock_files_dir)
    mock_retrieve_config_and_files.return_value = (
        mock_manifest_dict,
        mock_pipeline_config,
        mock_files_dir,
    )
    mock_error_handler.return_value = None
    mock_validate_pipeline.side_effect = ValueError("Invalid manifest file")
    with pytest.raises(ValueError, match="Invalid manifest file"):
        start("dummy_s3_object_name")


@patch("dpypelines.pipeline.utils._get_s3_client")
def test_download_zip_file(mock_get_s3_client, tmp_path):
    """Test that `download_zip_file()` downloads a zip file to the 'input' folder."""
    # Create a temporary directory to simulate the 'input' folder.
    input_dir = tmp_path / "input"
    input_dir.mkdir()

    # Create an in-memory zip file.
    zip_filename = "test.zip"
    fake_zip = io.BytesIO()
    with ZipFile(fake_zip, "w") as zf:
        zf.writestr("inside.txt", "Hello from zip")
    fake_zip.seek(0)

    # Configure the fake S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3
    mock_s3.download_fileobj.side_effect = lambda Bucket, Key, Fileobj: Fileobj.write(
        fake_zip.getvalue()
    )

    # Patch os.environ to simulate s3_object_name structure.
    s3_object_name = "bucket/input/" + zip_filename
    # Change the current working directory to tmp_path so that 'input' is created inside it.
    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        local_zip_path = download_zip_file(s3_object_name)
        # Verify that the zip file was downloaded to the 'input' folder.
        assert Path(local_zip_path).parent.name == "input"
        assert Path(local_zip_path).name == zip_filename
        # Verify the content by opening the zip file.
        with ZipFile(local_zip_path, "r") as zf:
            assert "inside.txt" in zf.namelist()
    finally:
        os.chdir(orig_cwd)


@patch("dpypelines.pipeline.utils.datetime")
@patch("dpypelines.pipeline.utils.upload_local_file_to_s3")
@patch("dpypelines.pipeline.utils.Path")
@patch("dpypelines.pipeline.utils._get_s3_client")
def test_upload_to_s3_processing_folder(
    mock_get_s3_client, mock_path, mock_upload, mock_timestamp
):
    """
    Test that `upload_to_s3_processing_folder()` uploads the original zip file and the unzipped contents to the S3 "processing" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    unzipped_file_paths = [Path("file/inside.txt")]
    mock_path = MagicMock(name="Path('file')")
    mock_path.parts = ["file"]
    mock_path.rglob.return_value = unzipped_file_paths

    # mock_timestamp.return_value.now.return_value.strftime.return_value = "testing"
    # mock_timestamp.return_value.now.return_value.strftime.side_effect = lambda x: ("")
    now = datetime.now()
    mock_timestamp.return_value.now.side_effect = lambda x: (now)

    s3_processing_folder = upload_to_s3_processing_folder(
        s3_object_name="bucket/key/file.zip",
        local_object_key="key/file.zip",
        decompressed_file_dir=mock_path,
    )
    copy_object_key = f"{s3_processing_folder}/key/file.zip"

    mock_s3.copy_object.assert_called_once_with(
        Bucket="bucket",
        Key=copy_object_key,
        CopySource={"Bucket": "bucket", "Key": "key/file.zip"},
    )
    mock_s3.delete_object.assert_called_once_with(Bucket="bucket", Key="key/file.zip")
    mock_path.rglob.assert_called_with("*")
    mock_upload.assert_called_once_with(
        f"{mock_path}/inside.txt",
        f"bucket/processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside.txt",
        None,
    )


@patch("dpypelines.pipeline.utils.datetime")
@patch("dpypelines.pipeline.utils.Path")
@patch("dpypelines.pipeline.utils._get_s3_client")
def test_copy_s3_processing_folder_to_processed_folder(
    mock_get_s3_client, mock_path, mock_timestamp
):
    """
    Tests that `copy_s3_processing_folder_to_s3_processed_folder()` copies all files from the S3 "processing" folder to the S3 "processed" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # TODO Mock copying of unzipped files
    unzipped_file_paths = [
        Path("file/inside.txt"),
    ]
    mock_path = MagicMock(name="Path('file')")
    mock_path.rglob.return_value = unzipped_file_paths

    mock_timestamp = MagicMock(name="datetime.now().strftime()")
    mock_timestamp.return_value = "timestamp-file"

    s3_processed_folder = copy_s3_processing_folder_to_processed_folder(
        s3_object_name="bucket/key/file.zip",
        decompressed_file_dir=mock_path,
        s3_processing_folder="processing/timestamp-file",
    )
    copy_calls = [
        call.copy_object(
            Bucket="bucket",
            Key=f"{s3_processed_folder}/inside.txt",
            CopySource={
                "Bucket": "bucket",
                "Key": "processing/timestamp-file/inside.txt",
            },
        ),
        call.copy_object(
            Bucket="bucket",
            Key=f"{s3_processed_folder}/key/file.zip",
            CopySource={
                "Bucket": "bucket",
                "Key": "processing/timestamp-file/key/file.zip",
            },
        ),
    ]

    mock_s3.assert_has_calls(copy_calls, any_order=True)
    mock_path.rglob.assert_called_with("*")


@patch("dpypelines.pipeline.utils.Path")
@patch("dpypelines.pipeline.utils._get_s3_client")
def test_delete_s3_processing_folder(mock_get_s3_client, mock_path):
    """
    Tests that `delete_s3_processing_folder()` deletes all files from the S3 "processing" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # TODO Mock deletion of unzipped files
    unzipped_file_paths = [
        Path("file/inside.txt"),
    ]
    mock_path = MagicMock(name="Path('file')")
    mock_path.rglob.return_value = unzipped_file_paths

    mock_timestamp = MagicMock(name="datetime.now().strftime()")
    mock_timestamp.return_value = "timestamp-file"
    delete_s3_processing_folder(
        s3_object_name="bucket/key/file.zip",
        decompressed_file_dir=mock_path,
        s3_processing_folder="processing/timestamp-file",
    )
    delete_calls = [
        call.delete_object(Bucket="bucket", Key="processing/timestamp-file/inside.txt"),
        call.delete_object(
            Bucket="bucket", Key="processing/timestamp-file/key/file.zip"
        ),
    ]
    mock_s3.assert_has_calls(delete_calls, any_order=True)
