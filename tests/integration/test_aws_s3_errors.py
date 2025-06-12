from unittest.mock import MagicMock, patch
import boto3
from moto import mock_aws
import pytest
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations
from tests.integration.mocks.mock_s3_client import create_mock_s3_client
from botocore.exceptions import ClientError


@patch("boto3.Session")
@mock_aws
def test_s3_download_error(
    mock_boto,
    zip_file_object_key_factory,
    s3_mock,
    setup_secrets,
    secretsmanager_mock,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """
    Tests when S3 download object fails
    """
    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "GetObject")  # type:ignore

    mock_get = MagicMock()
    mock_get.side_effect = raise_exception
    mock_s3_client = create_mock_s3_client(mock_s3_get=mock_get)

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "s3":
            return mock_s3_client
        return boto3.client(*args, **kwargs)

    mock_boto_session = MagicMock()
    mock_boto_session.client.side_effect = get_moto_client_mock
    mock_boto.return_value = mock_boto_session

    from dpypelines.s3_zip_received import start

    with pytest.raises(ClientError) as e:
        start(zip_file_object_key)

    assert e.value.operation_name == "GetObject"

    mock_s3_client.download_fileobj.assert_called()
    mock_s3_client.put_object.assert_not_called()

    mock_api_responses.assert_no_requests()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert (
        status["error_message"]
        == "An error occurred (AccessDenied) when calling the GetObject operation: Access Denied"
    )

    # Unexpected behaviour
    assert_no_success_and_one_failure(spy_notifier)

    # Not desired behaviour
    assert_no_emails_sent()
