from unittest.mock import MagicMock, patch
import boto3
from moto import mock_aws
import pytest
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi
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
        raise ClientError(error_response, "GetObject")

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

    from dpypelines.s3_folder_received import start

    with pytest.raises(ClientError) as e:
        start(zip_file_object_key)

    assert e.value.operation_name == "GetObject"
    mock_s3_client.download_fileobj.assert_called()
    mock_s3_client.put_object.assert_not_called()

    mock_dataset_api.assert_no_requests()

    # Unexpected behaviour
    assert_no_success_and_one_failure(spy_notifier)

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


# Copy failure
@patch("boto3.Session")
@mock_aws
def test_s3_copyobject_error(
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
    Tests when uploading file to S3 fails
    """

    zip_file_object_key = zip_file_object_key_factory()

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "CopyObject")

    mock_copy = MagicMock()
    mock_copy.side_effect = raise_exception
    mock_s3_client = create_mock_s3_client(mock_s3_copy=mock_copy)

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "s3":
            return mock_s3_client
        return boto3.client(*args, **kwargs)

    mock_boto_session = MagicMock()
    mock_boto_session.client.side_effect = get_moto_client_mock
    mock_boto.return_value = mock_boto_session

    from dpypelines.s3_folder_received import start

    with pytest.raises(ClientError) as e:
        start(zip_file_object_key)

    mock_dataset_api.assert_no_requests()

    assert e.value.operation_name == "CopyObject"

    mock_s3_client.download_fileobj.assert_called()
    mock_s3_client.copy_object.assert_called()

    # Unexpected behaviour
    spy_notifier.assert_called()
    spy_notifier_instance = spy_notifier.instance
    spy_notifier_instance.success.assert_not_called()

    # Not desired behaviour
    spy_notifier_instance.failure.assert_not_called()

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


# Delete failure
@patch("boto3.Session")
@mock_aws
def test_s3_deleteobject_error(
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
    Tests when deleting an S3 files
    """
    zip_file_object_key = zip_file_object_key_factory()

    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}

    def raise_exception(*args, **kwargs):
        raise ClientError(error_response, "DeleteObject")

    mock_delete = MagicMock()
    mock_delete.side_effect = raise_exception
    mock_s3_client = create_mock_s3_client(mock_s3_delete=mock_delete)

    def get_moto_client_mock(*args, **kwargs):
        if args[0] == "s3":
            return mock_s3_client
        return boto3.client(*args, **kwargs)

    mock_boto_session = MagicMock()
    mock_boto_session.client.side_effect = get_moto_client_mock
    mock_boto.return_value = mock_boto_session

    from dpypelines.s3_folder_received import start

    with pytest.raises(ClientError) as e:
        start(zip_file_object_key)

    mock_dataset_api.assert_no_requests()

    assert e.value.operation_name == "DeleteObject"

    mock_s3_client.download_fileobj.assert_called()
    mock_s3_client.copy_object.assert_called()
    mock_s3_client.delete_object.assert_called()

    # Unexpected behaviour
    spy_notifier.assert_called()
    spy_notifier_instance = spy_notifier.instance
    spy_notifier_instance.success.assert_not_called()

    # Not desired behaviour
    spy_notifier_instance.failure.assert_not_called()

    mock_upload_service.upload_new.assert_not_called()

    # File should still be in processing
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    head_object_result = mock_s3_client.actual_client.head_object(
        Bucket=uploaded_file_info.bucket_name,
        Key=uploaded_file_info.initial_key_without_bucket,
    )

    assert "Error" not in head_object_result

    uploaded_file_info.verify_s3_object_in_directory(
        mock_s3_client.actual_client, zip_file_object_key, "processing/", 2
    )
    # Not desired behaviour
    assert_no_emails_sent()
