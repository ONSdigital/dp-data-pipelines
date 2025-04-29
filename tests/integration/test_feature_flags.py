import boto3
from moto import mock_aws
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_no_emails_sent,
    assert_successful_email,
)
from tests.integration.helpers.upload_service_assertion_helpers import (
    validate_successful_upload_service_calls,
)


@mock_aws
def test_notifications_disabled(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
    monkeypatch,
):
    """
    Test notifications are not sent if disabled
    """
    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "True")
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()
    result = start(zip_file_object_key)

    # Assertions
    assert result is True

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get.assert_called_once()
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.post_json.assert_called_once()

    spy_notifier.assert_not_called()
    spy_notifier_instance = spy_notifier.instance
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_not_called()

    mock_upload_service.upload_new.assert_called()

    validate_successful_upload_service_calls(mock_upload_service)

    # Verify S3 operations - check if processing folder exists
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
        s3_client, expected_files, "processed/"
    )
    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "False")
    assert_successful_email()


@mock_aws
def test_emails_disabled(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
    monkeypatch,
):
    """
    Tests emails are not sent if disabled
    """
    monkeypatch.setenv("DISABLE_EMAILS", "True")

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()
    result = start(zip_file_object_key)

    # Assertions
    assert result is True

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get.assert_called_once()
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.post_json.assert_called_once()

    spy_notifier.assert_called_once()
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_called_once()
    spy_notifier_instance.failure.assert_not_called()

    mock_upload_service.upload_new.assert_called()

    validate_successful_upload_service_calls(mock_upload_service)

    # Verify S3 operations - check if processing folder exists
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
        s3_client, expected_files, "processed/"
    )

    assert_no_emails_sent()
    monkeypatch.setenv("DISABLE_EMAILS", "False")


# Test upload disabled
@mock_aws
def test_file_upload_disabled(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
    monkeypatch,
):
    """
    Tests uploads do not happen if disabled
    """
    monkeypatch.setenv("SKIP_DATA_UPLOAD", "True")
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()
    result = start(zip_file_object_key)

    # Assertions
    assert result is None

    assert len(mock_api_service_in_utils.instances) == 0

    spy_notifier.assert_called_once()
    spy_notifier_instance = spy_notifier.instance
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_not_called()

    mock_upload_service.upload_new.assert_not_called()

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = [
        "data.csv",
        "metadata.json",
        "manifest.json",
        uploaded_file_info.file_name,
    ]

    # Is this desired behaviour?
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "processing/"
    )

    assert_no_emails_sent()
    monkeypatch.setenv("DISABLE_EMAILS", "False")
