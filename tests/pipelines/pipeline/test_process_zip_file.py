import io
import os
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch
from zipfile import ZipFile

import pytest

from dpypelines.pipeline.process_zip_file import (
    copy_s3_processing_folder_to_destination_folder,
    decompress_zip_file,
    delete_s3_processing_folder,
    download_zip_file,
    process_zip_file,
    upload_to_s3_processing_folder,
)
from tests.pipelines.pipeline.mocks import (
    MockLocalDirectoryStore,
    mock_decompress_zip_file,
    mock_path_constructor,
)


@patch("dpypelines.pipeline.process_zip_file._get_s3_client")
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


@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("zipfile.ZipFile")
def test_decompress_zip_file_no_files(mock_zipfile, mock_path):
    """Test basic zip file decompression"""
    # Setup path mock with side effect to track instantiation arguments
    path_instances = {}

    def path_constructor(path_arg):
        return mock_path_constructor(path_arg, [], path_instances)

    mock_path.side_effect = path_constructor

    mock_zip_instance = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip_instance

    file_name = "test_basic"
    local_zip_path = f"/tmp/{file_name}.zip"
    result = decompress_zip_file(local_zip_path)

    mock_zipfile.assert_called_once_with(local_zip_path, "r")
    mock_zip_instance.extractall.assert_called_once()

    expected_folder = f"/tmp/{file_name}"
    assert expected_folder in path_instances

    mock_destination = path_instances[expected_folder]
    mock_destination.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_destination.rglob.assert_called_with("*")

    assert result == mock_destination


@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("zipfile.ZipFile")
def test_decompress_zip_file_non_recursive(mock_zipfile, mock_path):
    """Test basic zip file decompression"""
    # Setup path mock with side effect to track instantiation arguments
    path_instances = {}

    mock_file = MagicMock()
    mock_file.is_dir.return_value = False
    files = [mock_file]

    def path_constructor(path_arg):
        return mock_path_constructor(path_arg, files, path_instances)

    mock_path.side_effect = path_constructor

    mock_zip_instance = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip_instance

    file_name = "test_basic"
    local_zip_path = f"/tmp/{file_name}.zip"
    result = decompress_zip_file(local_zip_path)

    mock_zipfile.assert_called_once_with(local_zip_path, "r")
    mock_zip_instance.extractall.assert_called_once()

    expected_folder = f"/tmp/{file_name}"
    assert expected_folder in path_instances

    mock_destination = path_instances[expected_folder]
    mock_destination.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_destination.rglob.assert_called_with("*")

    assert result == mock_destination


@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("zipfile.ZipFile")
def test_decompress_zip_file_recursive(mock_zipfile, mock_path):
    """Test basic zip file decompression"""
    # Setup path mock with side effect to track instantiation arguments
    path_instances = {}

    mock_dir = MagicMock()
    mock_dir.is_dir.return_value = True
    files = [mock_dir]

    def path_constructor(path_arg):
        return mock_path_constructor(path_arg, files, path_instances)

    mock_path.side_effect = path_constructor

    mock_zip_instance = MagicMock()
    mock_zipfile.return_value.__enter__.return_value = mock_zip_instance

    file_name = "test_basic"
    local_zip_path = f"/tmp/{file_name}.zip"
    result = decompress_zip_file(local_zip_path)

    mock_zipfile.assert_called_once_with(local_zip_path, "r")
    mock_zip_instance.extractall.assert_called_once()
    expected_folder = f"/tmp/{file_name}"
    assert expected_folder in path_instances

    mock_destination = path_instances[expected_folder]
    mock_destination.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_destination.rglob.assert_called_with("*")

    assert result == mock_destination / mock_destination


@patch("dpypelines.pipeline.process_zip_file.datetime")
@patch("dpypelines.pipeline.process_zip_file.upload_local_file_to_s3")
@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("dpypelines.pipeline.process_zip_file._get_s3_client")
def test_upload_to_s3_processing_folder(
    mock_get_s3_client, mock_path, mock_upload, mock_timestamp
):
    """
    Test that `upload_to_s3_processing_folder()` uploads the original zip file and the unzipped contents to the S3 "processing" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # Configure mock decompressed file directory and unzipped file paths
    unzipped_file_paths = [Path("file/inside1.txt"), Path("file/inside2.txt")]
    mock_path = MagicMock(name="Path('file')")
    mock_path.parts = ["file"]
    mock_path.rglob.return_value = unzipped_file_paths

    # Configure mock timestamp
    now = datetime.now()
    mock_timestamp.now.return_value = now

    s3_processing_folder = upload_to_s3_processing_folder(
        s3_object_name="bucket/key/file.zip",
        local_object_key="key/file.zip",
        decompressed_file_dir=mock_path,
    )

    assert s3_processing_folder == f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file"
    mock_path.rglob.assert_called_with("*")
    upload_calls = [
        call(
            f"{mock_path}/inside1.txt",
            f"bucket/processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside1.txt",
            None,
        ),
        call(
            f"{mock_path}/inside2.txt",
            f"bucket/processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside2.txt",
            None,
        ),
    ]

    mock_upload.assert_has_calls(upload_calls, any_order=True)
    mock_s3.copy_object.assert_called_once_with(
        Bucket="bucket",
        Key=f"{s3_processing_folder}/key/file.zip",
        CopySource={"Bucket": "bucket", "Key": "key/file.zip"},
    )
    mock_s3.delete_object.assert_called_once_with(Bucket="bucket", Key="key/file.zip")


@patch("dpypelines.pipeline.process_zip_file.os.remove")
@patch("dpypelines.pipeline.process_zip_file.datetime")
@patch("dpypelines.pipeline.process_zip_file.upload_local_file_to_s3")
@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("dpypelines.pipeline.process_zip_file._get_s3_client")
def test_upload_to_s3_processing_folder_ignore_hidden_files(
    mock_get_s3_client, mock_path, mock_upload, mock_timestamp, mock_os_remove
):
    """
    Test that `upload_to_s3_processing_folder()` uploads the original zip file and the unzipped contents to the S3 "processing" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # Configure mock decompressed file directory and unzipped file paths
    unzipped_file_paths = [Path("file/.hidden.txt"), Path("file/inside.txt")]
    mock_path = MagicMock(name="Path('file')")
    mock_path.parts = ["file"]
    mock_path.rglob.return_value = unzipped_file_paths

    # Configure mock timestamp
    now = datetime.now()
    mock_timestamp.now.return_value = now

    mock_os_remove = MagicMock()
    mock_os_remove.return_value = None

    s3_processing_folder = upload_to_s3_processing_folder(
        s3_object_name="bucket/key/file.zip",
        local_object_key="key/file.zip",
        decompressed_file_dir=mock_path,
    )

    mock_path.rglob.assert_called_with("*")
    # Check that `upload_local_file_to_s3()` is only called once and ignores the hidden file ".hidden.txt"
    mock_upload.assert_called_once_with(
        f"{mock_path}/inside.txt",
        f"bucket/processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside.txt",
        None,
    )
    mock_s3.copy_object.assert_called_once_with(
        Bucket="bucket",
        Key=f"{s3_processing_folder}/key/file.zip",
        CopySource={"Bucket": "bucket", "Key": "key/file.zip"},
    )
    mock_s3.delete_object.assert_called_once_with(Bucket="bucket", Key="key/file.zip")


@patch("dpypelines.pipeline.process_zip_file.datetime")
@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("dpypelines.pipeline.process_zip_file._get_s3_client")
def test_copy_s3_processing_folder_to_processed_folder(
    mock_get_s3_client, mock_path, mock_timestamp
):
    """
    Tests that `copy_s3_processing_folder_to_s3_processed_folder()` copies all files from the S3 "processing" folder to the S3 "processed" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # Configure mock decompressed file directory and unzipped file paths
    unzipped_file_paths = [Path("file/inside1.txt"), Path("file/inside2.txt")]
    mock_path = MagicMock(name="Path('file')")
    mock_path.parts = ["file"]
    mock_path.rglob.return_value = unzipped_file_paths

    # Configure mock timestamp
    now = datetime.now()
    mock_timestamp.now.return_value = now

    s3_processed_folder = copy_s3_processing_folder_to_destination_folder(
        s3_object_name="bucket/key/file.zip",
        decompressed_file_dir=mock_path,
        s3_processing_folder=f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file",
        destination="processed",
    )

    assert s3_processed_folder == f"processed/{now.strftime('%y-%m-%dT%H-%M')}-file"
    mock_path.rglob.assert_called_with("*")
    copy_calls = [
        call.copy_object(
            Bucket="bucket",
            Key=f"{s3_processed_folder}/inside1.txt",
            CopySource={
                "Bucket": "bucket",
                "Key": f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside1.txt",
            },
        ),
        call.copy_object(
            Bucket="bucket",
            Key=f"{s3_processed_folder}/inside2.txt",
            CopySource={
                "Bucket": "bucket",
                "Key": f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside2.txt",
            },
        ),
        call.copy_object(
            Bucket="bucket",
            Key=f"{s3_processed_folder}/key/file.zip",
            CopySource={
                "Bucket": "bucket",
                "Key": f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file/key/file.zip",
            },
        ),
    ]
    mock_s3.assert_has_calls(copy_calls, any_order=True)


@patch("dpypelines.pipeline.process_zip_file.datetime")
@patch("dpypelines.pipeline.process_zip_file.Path")
@patch("dpypelines.pipeline.process_zip_file._get_s3_client")
def test_delete_s3_processing_folder(mock_get_s3_client, mock_path, mock_timestamp):
    """
    Tests that `delete_s3_processing_folder()` deletes all files from the S3 "processing" folder.
    """
    # Configure mock S3 client.
    mock_s3 = MagicMock()
    mock_get_s3_client.return_value = mock_s3

    # Configure mock decompressed file directory and unzipped file paths
    unzipped_file_paths = [
        Path("file/inside1.txt"),
        Path("file/inside2.txt"),
    ]
    mock_path = MagicMock(name="Path('file')")
    mock_path.rglob.return_value = unzipped_file_paths

    # Configure mock timestamp
    now = datetime.now()
    mock_timestamp.now.return_value = now

    delete_s3_processing_folder(
        s3_object_name="bucket/key/file.zip",
        decompressed_file_dir=mock_path,
        s3_processing_folder=f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file",
    )

    delete_calls = [
        call.delete_object(
            Bucket="bucket",
            Key=f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside1.txt",
        ),
        call.delete_object(
            Bucket="bucket",
            Key=f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file/inside2.txt",
        ),
        call.delete_object(
            Bucket="bucket",
            Key=f"processing/{now.strftime('%y-%m-%dT%H-%M')}-file/key/file.zip",
        ),
    ]
    mock_s3.assert_has_calls(delete_calls, any_order=True)


@patch("dpypelines.pipeline.process_zip_file.LocalDirectoryStore")
@patch("dpypelines.pipeline.process_zip_file.upload_to_s3_processing_folder")
@patch("dpypelines.pipeline.process_zip_file.decompress_zip_file")
@patch("dpypelines.pipeline.process_zip_file.download_zip_file")
def test_process_zip_file_success(
    mock_download, mock_decompress, mock_upload, mock_local_dir_store
):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = f"{folder_name}/{zip_filename}"

    s3_object_name = "bucket/input/dummy_s3_object"
    expected_decompressed_file_dir = str(zip_path)
    mock_download.return_value = expected_decompressed_file_dir

    def decompress(path: str):
        return mock_decompress_zip_file(path, zip_path)

    mock_upload.return_value = "processing/timestamp-sample"

    mock_decompress.side_effect = decompress

    zip_files = [
        "sample/inside.txt",
    ]

    def create_local_dir_store(path: str):
        return MockLocalDirectoryStore(path, folder_name, zip_files)

    mock_local_dir_store.side_effect = create_local_dir_store

    local_store, decompressed_file_dir, s3_processing_folder = process_zip_file(
        s3_object_name
    )

    mock_upload.assert_called_once_with(s3_object_name, zip_path, folder_name)
    assert decompressed_file_dir == folder_name
    assert s3_processing_folder == "processing/timestamp-sample"
    assert local_store.get_file_names() == zip_files

    # TOOD: check log messages


@patch("dpypelines.pipeline.process_zip_file.LocalDirectoryStore")
@patch("dpypelines.pipeline.process_zip_file.upload_to_s3_processing_folder")
@patch("dpypelines.pipeline.process_zip_file.decompress_zip_file")
@patch("dpypelines.pipeline.process_zip_file.download_zip_file")
def test_process_zip_file_errors_when_no_files(
    mock_download, mock_decompress, mock_upload, mock_local_dir_store
):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = f"{folder_name}/{zip_filename}"

    s3_object_name = "bucket/input/dummy_s3_object"
    expected_decompressed_file_dir = str(zip_path)
    mock_download.return_value = expected_decompressed_file_dir

    def decompress(path: str):
        return mock_decompress_zip_file(path, zip_path)

    mock_upload.return_value = "processing/timestamp-sample"

    mock_decompress.side_effect = decompress

    zip_files = []

    def create_local_dir_store(path: str):
        return MockLocalDirectoryStore(path, folder_name, zip_files)

    mock_local_dir_store.side_effect = create_local_dir_store

    with pytest.raises(FileNotFoundError) as e:
        local_store, decompressed_file_dir, s3_processing_folder = process_zip_file(
            s3_object_name
        )

    assert e.match(
        f"Decompressed directory {folder_name} is empty for s3_object_name {s3_object_name}."
    )
