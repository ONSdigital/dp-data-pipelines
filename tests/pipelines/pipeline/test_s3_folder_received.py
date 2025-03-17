import io
import os
import re
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest

from dpypelines.pipeline.utils import (
    clean_directory,
    decompress_zip_file,
    download_zip_file,
    move_extracted_folder,
    process_zip_file,
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
    mock_process_zip_file.return_value = mock_local_store
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


def test_clean_directory(tmp_path):
    """Test that `clean_directory()` deletes all files and subdirectories."""
    # Create sample files and subdirectories in tmp_path.
    (tmp_path / "file1.txt").write_text("content")
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / "file2.txt").write_text("more content")

    # Assert that the directory is not empty.
    assert any(tmp_path.iterdir())

    # Call clean_directory.
    clean_directory(tmp_path)

    # Assert that the directory is empty.
    assert not any(tmp_path.iterdir())


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
    mock_s3.download_fileobj.side_effect = lambda bucket, key, f: f.write(
        fake_zip.getvalue()
    )

    # Patch os.environ to simulate s3_object_name structure.
    s3_object_name = "bucket/" + zip_filename
    # Change the current working directory to tmp_path so that 'input' is created inside it.
    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        local_zip_path = download_zip_file(s3_object_name)
        # Verify that the zip file was downloaded to the 'input' folder.
        assert local_zip_path.parent.name == "input"
        assert local_zip_path.name == zip_filename
        # Verify the content by opening the zip file.
        with ZipFile(local_zip_path, "r") as zf:
            assert "inside.txt" in zf.namelist()
    finally:
        os.chdir(orig_cwd)


def test_decompress_zip_file_with_subfolder(tmp_path):
    """
    Test that when the zip file contains a subfolder,
    the function does not move the subfolder, keeping the original structure.
    """
    # Create a temporary zip file with a file inside a subfolder.
    zip_path = tmp_path / "test.zip"
    subfolder_name = "subfolder"
    file_inside = "inside.txt"
    zip_entry = f"{subfolder_name}/{file_inside}"
    with ZipFile(zip_path, "w") as zipf:
        zipf.writestr(zip_entry, "Hello world")

    # Define the destination directory.
    processing_dir = tmp_path / "processing"
    
    # Call the decompression function.
    decompress_zip_file(zip_path, dest_folder=processing_dir)

    # Since the zip already had a subfolder, the function should leave the structure intact.
    expected_subfolder = processing_dir / subfolder_name
    extracted_file = expected_subfolder / file_inside

    assert expected_subfolder.exists() and expected_subfolder.is_dir(), "Subfolder missing after extraction."
    assert extracted_file.exists(), "Extracted file not found in the subfolder."
    assert extracted_file.read_text() == "Hello world"


def test_move_extracted_folder(tmp_path):
    """Test that `move_extracted_folder()` moves the folder from 'processing' to 'processed'."""
    # Create a 'processing' folder with a folder named 'sample' (extracted folder).
    processing_dir = tmp_path / "processing"
    processing_dir.mkdir()
    folder_name = "sample"
    extracted_folder = processing_dir / folder_name
    extracted_folder.mkdir()
    (extracted_folder / "dummy.txt").write_text("dummy content")

    # Create a 'processed' folder.
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()

    # Call move_extracted_folder with a zip filename 'sample.zip'
    move_extracted_folder("sample.zip", src_dir=processing_dir, dest_dir=processed_dir)

    # Verify that the folder was moved.
    dest_folder = processed_dir / folder_name
    assert dest_folder.exists()
    assert (dest_folder / "dummy.txt").exists()
    # The original folder should no longer exist.
    assert not extracted_folder.exists()


@patch("dpypelines.pipeline.utils.download_zip_file")
@patch("dpypelines.pipeline.utils.decompress_zip_file")
@patch("dpypelines.pipeline.utils.move_extracted_folder")
def test_process_zip_file(mock_move, mock_decompress, mock_download, tmp_path):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    # Create a temporary zip file in tmp_path.
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = tmp_path / zip_filename
    with ZipFile(zip_path, "w") as zipf:
        # Create a folder inside the zip with one file.
        zipf.writestr(f"{folder_name}/inside.txt", "sample content")

    # Configure mocks:
    mock_download.return_value = zip_path

    # Simulate decompression: extract the zip into a "processing" folder under tmp_path.
    def decompress_side_effect(zip_path_arg, dest_folder):
        dest_folder = Path(dest_folder)
        dest_folder.mkdir(parents=True, exist_ok=True)
        with ZipFile(zip_path_arg, "r") as zip_ref:
            zip_ref.extractall(dest_folder)

    mock_decompress.side_effect = decompress_side_effect

    # Simulate move: move the folder from processing to processed.
    def move_side_effect(zip_filename_arg, src_dir, dest_dir):
        src_folder = Path(src_dir) / Path(zip_filename_arg).stem
        dest_folder = Path(dest_dir) / Path(zip_filename_arg).stem
        if dest_folder.exists():
            shutil.rmtree(dest_folder)
        shutil.move(str(src_folder), str(dest_folder))

    mock_move.side_effect = move_side_effect

    # Change current working directory to tmp_path for isolation.
    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        local_store = process_zip_file("dummy_s3_object")
        # Verify that the processed folder contains the expected file.
        files = local_store.get_file_names()
        assert any("inside.txt" in file for file in files)
    finally:
        os.chdir(orig_cwd)
