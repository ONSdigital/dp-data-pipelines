import boto3
from moto import mock_aws
import pytest
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_successful_email,
)
from tests.integration.helpers.upload_service_assertion_helpers import (
    validate_successful_upload_service_calls,
)

files_to_generate = ["csvfile.csv", "sqlite.csdb", "excel.xls", "otherexcel.xlsx"]


@pytest.mark.parametrize("data_file_name", files_to_generate)
@mock_aws
def test_successful_pipeline_execution(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
    data_file_name,
):
    """
    Test a successful pipeline execution with mocked services.
    """

    zip_file_object_key = zip_file_object_key_factory(data_file_name=data_file_name)

    from dpypelines.s3_folder_received import start

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

    validate_successful_upload_service_calls(
        mock_upload_service, data_file_name=data_file_name
    )

    # Verify S3 operations - check if processing folder exists
    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processed/"
    )

    assert_successful_email()
