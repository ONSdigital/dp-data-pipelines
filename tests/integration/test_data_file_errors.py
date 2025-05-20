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
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_exception_email_sent,
)
from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi


@mock_aws
def test_missing_data(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
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

    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing"
    )
    assert_exception_email_sent(str(e.value))


@mock_aws
def test_empty_data(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
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

    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing"
    )

    assert_exception_email_sent(str(e.value))


@mock_aws
def test_unsupported_filetype(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
):
    """Test that an unsupported filetype errors."""
    data_contents = {"somekey": "somevalue"}
    extension = "not-a-real-file"
    data_file_name = f"data.{extension}"
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        data_config=FileGenerationConfig(
            include=True, content=json.dumps(data_contents)
        ),
        data_file_name=data_file_name,
    )

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "File format validation failed for" in str(e.value)
    assert f"Extension {extension} is not supported" in str(e.value)

    assert_no_success_and_one_failure(spy_notifier)
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/"
    )

    assert_exception_email_sent(str(e.value))
