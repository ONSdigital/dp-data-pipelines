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
def test_secretsmanager_error(
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
    Tests when secrets manager errors
    """
    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "GetSecretValue")  # type:ignore

    secrets_manager_mock = MagicMock()
    secrets_manager_mock.get_secret_value.side_effect = raise_exception

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "secretsmanager":
            return secrets_manager_mock
        return boto3.client(*args, **kwargs)

    mock_boto.side_effect = get_moto_client_mock

    from dpypelines.s3_zip_received import start

    # Not desired behviour - should raise custom exception
    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert (
        "An error occurred (AccessDenied) when calling the GetSecretValue operation"
        in str(e.value)
    )

    mock_api_responses.assert_no_requests()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    # Pipeline failed before first database operation
    assert dataset is None

    # Is this what it should be?
    spy_notifier.assert_not_called()
    assert len(spy_notifier.instances) == 0

    # Not desired behaviour
    assert_no_emails_sent()
