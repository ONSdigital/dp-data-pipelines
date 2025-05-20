from unittest.mock import MagicMock
import boto3
import pytest
from moto import mock_aws
from requests.exceptions import RequestException
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.s3_assertion_helpers import (
    S3ObjectFile,
)
from tests.integration.helpers.ses_assertion_helpers import (
    assert_exception_email_sent,
    assert_successful_email,
)
from tests.integration.helpers.upload_service_assertion_helpers import (
    validate_successful_upload_service_calls,
)


@mock_aws
def test_upload_service_request_exception(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """
    Test dataset type that isn't static
    """

    def throw_error(required_file_path, mimetype):
        raise RequestException()

    mock_upload_service.upload_new.side_effect = throw_error

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    with pytest.raises(RequestException) as e:
        start(zip_file_object_key)

    assert_no_success_and_one_failure(spy_notifier)

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get.assert_called_once()
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.post_json.assert_called_once()

    mock_upload_service.upload_new.assert_called_once()

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/"
    )

    # Not desired behaviour
    assert_exception_email_sent(str(e.value))


@mock_aws
def test_upload_service_returns_error(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """
    Test dataset type that isn't static
    """
    response_mock = MagicMock()
    response_mock.status_code = 500
    response_mock.text = "Some failure message here"
    mock_upload_service.upload_new.return_value = response_mock

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    result = start(zip_file_object_key)

    # Not desired behaviour but is current behaviour
    assert result

    spy_notifier.instances[0].success.assert_called_once()
    spy_notifier.instances[0].failure.assert_not_called()
    # END not desired behaviour

    assert len(mock_api_service_in_utils.instances) == 1

    mock_api_client_instance = mock_api_service_in_utils.instances[0]
    mock_api_client_instance.get.assert_called_once()
    mock_api_client_instance.get_path.assert_called_once()
    mock_api_client_instance.post_json.assert_called_once()

    validate_successful_upload_service_calls(mock_upload_service)
    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processed/"
    )

    # Not desired behaviour
    assert_successful_email()
