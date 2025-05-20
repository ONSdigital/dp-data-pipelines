from unittest.mock import MagicMock, patch
import boto3
from moto import mock_aws
import pytest
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from tests.integration.helpers.upload_service_assertion_helpers import (
    validate_successful_upload_service_calls,
)
from botocore.exceptions import ClientError

from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi

actual_boto_client = boto3.client


@patch("boto3.client")
@mock_aws
def test_ses_sendemail_error(
    mock_boto,
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
    Test when an error is thrown by AWS SES client
    """

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "SendEmail")

    ses_mock = MagicMock()
    ses_mock.send_email.side_effect = raise_exception

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "ses":
            return ses_mock
        return actual_boto_client(*args, **kwargs)

    mock_boto.side_effect = get_moto_client_mock
    zip_file_object_key = zip_file_object_key_factory()

    from dpypelines.s3_folder_received import start

    with pytest.raises(ClientError) as e:
        start(zip_file_object_key)

    assert e.value.operation_name == "SendEmail"

    mock_dataset_api.assert_all_requests_made()

    assert len(spy_notifier.call_args_list) == 1
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_called_once()

    mock_upload_service.upload_new.assert_called()

    validate_successful_upload_service_calls(mock_upload_service)

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    # verify the 2
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/", 2
    )

    assert_no_emails_sent()
