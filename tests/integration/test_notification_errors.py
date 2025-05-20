import boto3
from moto import mock_aws
import pytest
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    get_sent_emails,
)
from tests.integration.helpers.upload_service_assertion_helpers import (
    validate_successful_upload_service_calls,
)
from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi


@mock_aws
def test_slack_notification_error(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
):
    """
    Tests when a Slack notification fails
    """

    def throw_error(error_message: str):
        raise Exception(error_message)

    mock_slack.msg_str.side_effect = throw_error
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    with pytest.raises(Exception):
        start(zip_file_object_key)

    mock_dataset_api.assert_all_requests_made()

    assert len(spy_notifier.call_args_list) == 1
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_called_once()
    spy_notifier_instance.failure.assert_called_once()

    mock_upload_service.upload_new.assert_called()

    validate_successful_upload_service_calls(mock_upload_service)

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/", 2
    )
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processed/", 2
    )

    # Not desired behaviour
    sent_emails = get_sent_emails()
    assert len(sent_emails) == 2
