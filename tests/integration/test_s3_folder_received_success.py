import boto3
from moto import mock_aws
import pytest
from dpypelines.pipeline.models.metadata_models import DatasetVersion
from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
    generate_metadata_dict,
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_successful_email,
)
from tests.integration.helpers.upload_service_assertion_helpers import (
    validate_successful_upload_service_calls,
)
from tests.integration.mocks.mock_dataset_api_client import (
    MockDatasetApi,
    mock_versions,
)

files_to_generate = ["csvfile.csv", "sqlite.csdb", "excel.xls", "otherexcel.xlsx"]


@pytest.mark.parametrize("data_file_name", files_to_generate)
@mock_aws
def test_successful_pipeline_execution_using_existing_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    data_file_name,
):
    """
    Test a successful pipeline execution with mocked services.
    """
    keys_to_remove_from_metadata = ["alerts", "quality_designation", "usage_notes"]
    zip_file_object_key = zip_file_object_key_factory(
        data_file_name=data_file_name,
        metadata_config=FileGenerationConfig(
            missing_field_keys=keys_to_remove_from_metadata
        ),
    )

    new_metadata = generate_metadata_dict(data_file_name)
    for key in keys_to_remove_from_metadata:
        del new_metadata[key]
    original_metadata = mock_versions[0]

    combined_metadata = original_metadata.copy()
    combined_metadata.update(new_metadata)
    combined_metadata = DatasetVersion(**combined_metadata).model_dump()
    mock_dataset_api.mock_post_versions(request_body=combined_metadata)

    from dpypelines.s3_folder_received import start

    result = start(zip_file_object_key)

    # Assertions
    assert result is True

    mock_dataset_api.assert_all_requests_made()

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


@pytest.mark.parametrize("data_file_name", files_to_generate)
@mock_aws
def test_successful_pipeline_execution_with_new_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    data_file_name,
):
    """
    Test a successful pipeline execution with mocked services.
    """
    expected_metadata = DatasetVersion(
        **generate_metadata_dict(data_file_name)
    ).model_dump()

    mock_dataset_api.mock_post_versions(request_body=expected_metadata)
    zip_file_object_key = zip_file_object_key_factory(
        data_file_name=data_file_name,
        manifest_config=FileGenerationConfig(
            missing_field_keys=["use_previous_metadata"]
        ),
    )

    from dpypelines.s3_folder_received import start

    result = start(zip_file_object_key)

    # Assertions
    assert result is True

    mock_dataset_api.assert_all_requests_made()
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
