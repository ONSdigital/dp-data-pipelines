import json
import boto3
import pytest
from moto import mock_aws

from tests.integration.helpers.file_helpers import (
    DATA_FILE_NAME,
    FileGenerationConfig,
)
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
    assert_success_notification,
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_exception_email_sent,
    assert_successful_email,
)


@mock_aws
def test_missing_data(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        data_config=FileGenerationConfig(include=False)
    )

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert DATA_FILE_NAME in str(e)

    assert_no_success_and_one_failure(spy_notifier)

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = ["metadata.json", "manifest.json", uploaded_file_info.file_name]
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "processing/"
    )

    # Not desired behaviour
    assert_exception_email_sent(str(e.value))


@mock_aws
def test_empty_data(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        data_config=FileGenerationConfig(include=True, empty=True)
    )

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert_no_success_and_one_failure(spy_notifier)
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
def test_unsupported_filetype(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    data_contents = {
        "somekey": "somevalue"
    }
    data_file_name = "data.json"
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        data_config=FileGenerationConfig(include=True, content=json.dumps(data_contents)),
        data_file_name=data_file_name
    )

    # The following assertions are likely incorrect, but match current implementation
    result = start(zip_file_object_key)
    assert result
    assert_success_notification(spy_notifier)
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = [
        data_file_name,
        "metadata.json",
        "manifest.json",
        uploaded_file_info.file_name,
    ]
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "processed/"
    )
    
    # Not desired behaviour
    assert_successful_email()