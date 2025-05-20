from unittest.mock import MagicMock, patch
import boto3
from moto import mock_aws
import pytest
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from botocore.exceptions import ClientError

from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi

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
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
):
    """
    Tests when S3 download object fails
    """
    zip_file_object_key = zip_file_object_key_factory()

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "GetSecretValue")

    secrets_manager_mock = MagicMock()
    secrets_manager_mock.get_secret_value.side_effect = raise_exception

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "secretsmanager":
            return secrets_manager_mock
        return boto3.client(*args, **kwargs)

    mock_boto.side_effect = get_moto_client_mock

    from dpypelines.s3_folder_received import start

    # Not desired behviour - should raise custom exception
    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "Failed to retrieve secrets from AWS Secrets Manager" in str(e.value)

    mock_dataset_api.assert_no_requests()

    # Is this what it should be?
    spy_notifier.assert_called()
    assert len(spy_notifier.instances) == 0

    mock_upload_service.upload_new.assert_not_called()

    # File should still be in processing
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    head_object_result = mock_boto.head_object(
        Bucket=uploaded_file_info.bucket_name,
        Key=uploaded_file_info.initial_key_without_bucket,
    )

    assert "Error" not in head_object_result

    # Not desired behaviour
    assert_no_emails_sent()
