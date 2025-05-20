import boto3
from pydantic import ValidationError
import pytest
from moto import mock_aws

from tests.integration.helpers.file_helpers import (
    METADATA_FILE_NAME,
    FileGenerationConfig,
)
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import (
    assert_exception_email_sent,
    assert_successful_email,
)
from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi


@mock_aws
def test_missing_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
):
    """Test pipeline handles missing metadata file"""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(include=False)
    )

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert METADATA_FILE_NAME in str(e)
    assert_no_success_and_one_failure(spy_notifier)

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/"
    )


@mock_aws
def test_empty_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
):
    """Test pipeline handles an empty metadata file."""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(include=True, empty=True)
    )

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "File is empty" in str(e)
    assert METADATA_FILE_NAME in str(e)
    assert_no_success_and_one_failure(spy_notifier)

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/"
    )
    # Not desired behaviour
    assert_exception_email_sent(str(e.value))


required_metadata_fields = [
    "dataset_id",
    "release_date",
    "edition",
    "distributions",
]


@pytest.mark.parametrize("field_to_remove", required_metadata_fields)
@mock_aws
def test_metadata_fails_when_missing_required_field(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    field_to_remove,
):
    """Test a pipeline handles errors when metadata is missing required fields"""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(
            include=True, missing_field_keys=[field_to_remove]
        )
    )

    with pytest.raises(ValidationError) as e:
        start(zip_file_object_key)

    assert field_to_remove in str(e.value)
    assert_no_success_and_one_failure(spy_notifier)

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/"
    )
    # Not desired behaviour
    assert_exception_email_sent(str(e.value))


metadata_optional_fields = [
    "quality_designation",
    "usage_notes",
    "alerts",
    "edition_title",
]


@pytest.mark.parametrize("field_to_remove", metadata_optional_fields)
@mock_aws
def test_metadata_succeeds_when_missing_optional_field(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    field_to_remove,
):
    """Test pipeline still works when metadata is missing optional fields"""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(
            include=True, missing_field_keys=[field_to_remove]
        )
    )

    result = start(zip_file_object_key)
    assert result

    assert len(spy_notifier.instances) == 1
    spy_notifier.instances[0].success.assert_called_once()

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)
    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processed/"
    )

    assert_successful_email()


@mock_aws
def test_metadata_fails_when_invalid_json(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(include=True, content="this is not json")
    )

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "not valid JSON" in str(e.value), str(e.value)
    assert "metadata.json" in str(e.value), str(e.value)

    assert_no_success_and_one_failure(spy_notifier)

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    uploaded_file_info.verify_s3_object_in_directory(
        s3_client, zip_file_object_key, "processing/"
    )
    # Couldn't read metadata so no submitter info to notify
    assert_exception_email_sent(str(e.value))
