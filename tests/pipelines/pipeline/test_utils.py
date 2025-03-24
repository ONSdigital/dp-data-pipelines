from unittest.mock import MagicMock, patch
import pytest
from dpypelines.pipeline.utils import decompress_zip_file, process_zip_file
from tests.pipelines.pipeline.mocks import MockLocalDirectoryStore, mock_path_constructor, mock_decompress_zip_file

@patch("dpypelines.pipeline.utils.Path")
@patch('zipfile.ZipFile')
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
    
    assert file_name in path_instances

    mock_destination = path_instances[file_name]
    mock_destination.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_destination.rglob.assert_called_with("*")

    assert result == mock_destination

@patch("dpypelines.pipeline.utils.Path")
@patch('zipfile.ZipFile')
def test_decompress_zip_file_non_recursive(mock_zipfile, mock_path):
    """Test basic zip file decompression"""
    # Setup path mock with side effect to track instantiation arguments
    path_instances = {}
    
    csv_mock = MagicMock()
    csv_mock.is_dir.return_value = False
    files = [
        csv_mock
    ]
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
    
    assert file_name in path_instances

    mock_destination = path_instances[file_name]
    mock_destination.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_destination.rglob.assert_called_with("*")

    assert result == mock_destination

@patch("dpypelines.pipeline.utils.Path")
@patch('zipfile.ZipFile')
def test_decompress_zip_file_recursive(mock_zipfile, mock_path):
    """Test basic zip file decompression"""
    # Setup path mock with side effect to track instantiation arguments
    path_instances = {}
    
    csv_mock = MagicMock()
    csv_mock.is_dir.return_value = True
    files = [
        csv_mock
    ]

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
    
    assert file_name in path_instances

    mock_destination = path_instances[file_name]
    mock_destination.mkdir.assert_called_once_with(parents=True, exist_ok=True)
    mock_destination.rglob.assert_called_with("*")

    assert result == mock_destination / mock_destination


@patch("dpypelines.pipeline.utils.LocalDirectoryStore")
@patch("dpypelines.pipeline.utils.upload_to_s3_processing_folder")
@patch("dpypelines.pipeline.utils.decompress_zip_file")
@patch("dpypelines.pipeline.utils.download_zip_file")
def test_process_zip_file_success(mock_download, mock_decompress, mock_upload, mock_local_dir_store):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = f"{folder_name}/{zip_filename}"

    s3_object_name =  "bucket/input/dummy_s3_object"
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

    assert decompressed_file_dir == folder_name
    mock_upload.assert_called_once_with(s3_object_name, zip_path, folder_name)
    assert s3_processing_folder == "processing/timestamp-sample" 
    assert local_store.get_file_names() == zip_files

    # TOOD: check log messages
    
@patch("dpypelines.pipeline.utils.LocalDirectoryStore")
@patch("dpypelines.pipeline.utils.upload_to_s3_processing_folder")
@patch("dpypelines.pipeline.utils.decompress_zip_file")
@patch("dpypelines.pipeline.utils.download_zip_file")
def test_process_zip_file_errors_when_no_files(mock_download, mock_decompress, mock_upload, mock_local_dir_store):
    """Test that `process_zip_file()` processes the zip file and verifies its content."""
    zip_filename = "sample.zip"
    folder_name = "sample"
    zip_path = f"{folder_name}/{zip_filename}"

    s3_object_name =  "bucket/input/dummy_s3_object"
    expected_decompressed_file_dir = str(zip_path)
    mock_download.return_value = expected_decompressed_file_dir
    def decompress(path: str):
        return mock_decompress_zip_file(path, zip_path)

    mock_upload.return_value = "processing/timestamp-sample" 

    mock_decompress.side_effect = decompress

    zip_files = [
    ]

    def create_local_dir_store(path: str):
        return MockLocalDirectoryStore(path, folder_name, zip_files)
    
    mock_local_dir_store.side_effect = create_local_dir_store

    with pytest.raises(FileNotFoundError) as e:        
        local_store, decompressed_file_dir, s3_processing_folder = process_zip_file(
        s3_object_name
        )

    assert e.match(f"Decompressed directory {folder_name} is empty for s3_object_name {s3_object_name}.")