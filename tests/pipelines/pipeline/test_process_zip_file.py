import io
import os
from pathlib import Path
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest

from dpypelines.pipeline.process_zip_file import (
    decompress_zip_file,
    download_zip_file,
    process_zip_file,
    S3Object,
)
from tests.pipelines.pipeline.mocks import (
    MockLocalDirectoryStore,
    mock_decompress_zip_file,
    mock_path_constructor,
)


@patch("dpypelines.pipeline.process_zip_file.S3Object")
@patch("dpypelines.pipeline.process_zip_file._get_s3_client")
def test_download_zip_file(mock_get_s3_client, mock_s3_object, tmp_path):
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

    mock_s3_object = S3Object("bucket/input/test.zip")

    # Change the current working directory to tmp_path so that 'input' is created inside it.
    orig_cwd = os.getcwd()
    os.chdir(tmp_path)
    try:
        local_zip_path = download_zip_file(mock_s3_object)
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


@patch("dpypelines.pipeline.process_zip_file.S3Object")
@patch("dpypelines.pipeline.process_zip_file.LocalDirectoryStore")
@patch("dpypelines.pipeline.process_zip_file.decompress_zip_file")
@patch("dpypelines.pipeline.process_zip_file.download_zip_file")
def test_process_zip_file_success(
    mock_download, mock_decompress, mock_local_dir_store, mock_s3_object
):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = f"{folder_name}/{zip_filename}"
    mock_s3_object = MagicMock()

    expected_decompressed_file_dir = str(zip_path)
    mock_download.return_value = expected_decompressed_file_dir

    def decompress(path: str):
        return mock_decompress_zip_file(path, zip_path)

    mock_decompress.side_effect = decompress

    zip_files = [
        "sample/inside.txt",
    ]

    def create_local_dir_store(path: str):
        return MockLocalDirectoryStore(Path(path), folder_name, zip_files)

    mock_local_dir_store.side_effect = create_local_dir_store

    local_store, decompressed_file_dir = process_zip_file(mock_s3_object)

    assert decompressed_file_dir == folder_name
    assert local_store.get_file_names() == zip_files

    # TOOD: check log messages


@patch("dpypelines.pipeline.process_zip_file.S3Object")
@patch("dpypelines.pipeline.process_zip_file.LocalDirectoryStore")
@patch("dpypelines.pipeline.process_zip_file.decompress_zip_file")
@patch("dpypelines.pipeline.process_zip_file.download_zip_file")
def test_process_zip_file_errors_when_no_files(
    mock_download, mock_decompress, mock_local_dir_store, mock_s3_object
):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = f"{folder_name}/{zip_filename}"

    mock_s3_object = MagicMock()
    mock_s3_object.key = zip_path
    expected_decompressed_file_dir = str(zip_path)
    mock_download.return_value = expected_decompressed_file_dir

    def decompress(path: str):
        return mock_decompress_zip_file(path, zip_path)

    mock_decompress.side_effect = decompress

    zip_files = []

    def create_local_dir_store(path: str):
        return MockLocalDirectoryStore(Path(path), folder_name, zip_files)

    mock_local_dir_store.side_effect = create_local_dir_store

    with pytest.raises(FileNotFoundError) as e:
        decompressed_file_dir, s3_processing_folder = process_zip_file(mock_s3_object)

    assert e.match(
        f"Decompressed directory {folder_name} is empty for s3_object_key {mock_s3_object.key}."
    )
