from unittest.mock import MagicMock, patch
import boto3
from moto import mock_aws
import pytest
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from botocore.exceptions import ClientError

from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations

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
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """
    Test when an error is thrown by AWS SES client
    """

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "SendEmail")  # type:ignore

    ses_mock = MagicMock()
    ses_mock.send_email.side_effect = raise_exception

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "ses":
            return ses_mock
        return actual_boto_client(*args, **kwargs)

    mock_boto.side_effect = get_moto_client_mock
    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    from dpypelines.s3_zip_received import start

    with pytest.raises(ClientError) as e:
        start(zip_file_object_key)

    assert e.value.operation_name == "SendEmail"

    mock_api_responses.assert_all_requests_made()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "COMPLETED"
    assert len(status["events"]) == 5

    assert len(spy_notifier.call_args_list) == 1
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_called_once()

    assert_no_emails_sent()
