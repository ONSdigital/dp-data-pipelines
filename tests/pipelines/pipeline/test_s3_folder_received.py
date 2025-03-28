import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.s3_folder_received import start


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
