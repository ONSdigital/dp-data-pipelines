import json
import pytest
from moto import mock_aws

from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.file_helpers import (
    DATA_FILE_NAME,
    FileGenerationConfig,
)
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.ses_assertion_helpers import (
    assert_email_sent,
)
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_missing_data(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        data_config=FileGenerationConfig(include=False)
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert DATA_FILE_NAME in str(e)

    mock_db_operations.assert_failed_status_new_dataset(
        dataset_id=s3_object.dataset_id,
        event_count=3,
        err_msg="Required file not found: data.csv",
    )

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)

    assert_email_sent(
        expected_content_parts=[
            "An error has occurred:",
            "Required file not found: data.csv",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        expected_subject="ETL Pipeline error has occurred",
    )


@mock_aws
def test_empty_data(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        data_config=FileGenerationConfig(include=True, empty=True)
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "File is empty: data.csv" in str(e)
    mock_db_operations.assert_failed_status_new_dataset(
        dataset_id=s3_object.dataset_id,
        event_count=3,
        err_msg="File is empty: data.csv",
    )

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)

    assert_email_sent(
        expected_content_parts=[
            "An error has occurred:",
            "File is empty: data.csv",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        expected_subject="ETL Pipeline error has occurred",
    )


@mock_aws
def test_unsupported_filetype(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test that an unsupported filetype errors."""
    data_contents = {"somekey": "somevalue"}
    extension = "not-a-real-file"
    data_file_name = f"data.{extension}"
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        data_config=FileGenerationConfig(
            include=True, content=json.dumps(data_contents)
        ),
        data_file_name=data_file_name,
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "File format validation failed for" in str(e.value)
    assert f"Extension {extension} is not supported" in str(e.value)

    mock_db_operations.assert_failed_status_new_dataset(
        dataset_id=s3_object.dataset_id,
        event_count=3,
        err_msg="File format validation failed for data.not-a-real-file: Extension not-a-real-file is not supported",
    )

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)

    assert_email_sent(
        expected_content_parts=[
            "An error has occurred:",
            "File format validation failed for data.not-a-real-file: Extension not-a-real-file is not supported",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        expected_subject="ETL Pipeline error has occurred",
    )
