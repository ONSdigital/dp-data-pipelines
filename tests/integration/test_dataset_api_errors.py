import boto3
import pytest
from moto import mock_aws
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_exception_email_sent,
    assert_no_emails_sent,
)


@mock_aws
def test_non_static_dataset(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """
    Test dataset type that isn't static
    """
    # Run the pipeline with the S3 object name
    mock_api_service_in_utils.mock_config.mock_get_response.text = (
        '{"current": {"type": "not-static"}}'
    )

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()
    result = start(zip_file_object_key)

    # Assertions
    assert result is False

    assert len(spy_notifier.instances) == 1
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_not_called()

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.get.assert_called_once()

    mock_api_client_instance.post_json.assert_not_called()

    mock_upload_service.upload_new.assert_not_called()

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = [
        "data.csv",
        "metadata.json",
        "manifest.json",
        uploaded_file_info.file_name,
    ]
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "dataset-type-not-static/"
    )

    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_get_path_404_error(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """
    Test 404 error retrieving dataset version from Dataset API
    """
    # Run the pipeline with the S3 object name
    mock_api_service_in_utils.mock_config.mock_get_path_response.text = None
    mock_api_service_in_utils.mock_config.mock_get_path_response.status_code = 404

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()
    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert_no_success_and_one_failure(spy_notifier)

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.get.assert_not_called()
    mock_api_client_instance.post_json.assert_not_called()

    mock_upload_service.upload_new.assert_not_called()

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = [
        "data.csv",
        "metadata.json",
        "manifest.json",
        uploaded_file_info.file_name,
    ]
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "processing/"
    )

    # Not desired behaviour
    assert_exception_email_sent(str(e.value))


@mock_aws
def test_post_json_error(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """
    Test when dataset API has a non-success response when uploading the metadata
    """
    # Run the pipeline with the S3 object name
    mock_api_service_in_utils.mock_config.mock_post_json_response.text = None
    mock_api_service_in_utils.mock_config.mock_post_json_response.status_code = 500

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "Mock error raised as mock" in str(e.value)

    assert_no_success_and_one_failure(spy_notifier)

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get.assert_called_once()
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.post_json.assert_called_once()

    mock_upload_service.upload_new.assert_not_called()

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = [
        "data.csv",
        "metadata.json",
        "manifest.json",
        uploaded_file_info.file_name,
    ]
    # Not right behaviour - should be in errored
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "processing/"
    )
    # Not desired behaviour
    assert_exception_email_sent(str(e.value))
