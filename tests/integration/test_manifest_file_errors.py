from json import JSONDecodeError
import boto3
import pytest
from moto import mock_aws

from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
)
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.s3_assertion_helpers import S3ObjectFile
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent


@mock_aws
def test_missing_manifest(
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
    Manifest file missing from zip file
    """
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(include=False)
    )

    with pytest.raises(FileNotFoundError) as e:
        start(zip_file_object_key)

    assert "Failed to retrieve manifest from the local directory store" in str(e.value)
    assert_no_success_and_one_failure(spy_notifier)

    s3_client = boto3.client("s3", region_name="eu-west-2")
    uploaded_file_info = S3ObjectFile(zip_file_object_key)
    uploaded_file_info.verify_file_moved(s3_client)

    expected_files = ["data.csv", "metadata.json", uploaded_file_info.file_name]
    uploaded_file_info.verify_files_in_destination(
        s3_client, expected_files, "processing/"
    )

    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_empty_manifest(
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
    Manifst file empty.
    """
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(include=True, empty=True)
    )

    # This is the wrong expected behaviour, but it's what's actually happening currently.
    with pytest.raises(JSONDecodeError):
        start(zip_file_object_key)

    assert_no_success_and_one_failure(spy_notifier)

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
        s3_client, expected_files, "processing/"
    )
    # Not desired behaviour
    assert_no_emails_sent()


required_manifest_fields = [
    "metadata_file",
    "submission_contacts",
]


@pytest.mark.parametrize("field_to_remove", required_manifest_fields)
@mock_aws
def test_manifest_missing_fields(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
    field_to_remove,
):
    """
    Manifest file missing fields.
    """
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(
            include=True, missing_field_keys=[field_to_remove]
        )
    )

    # This is the wrong expected behaviour, but it's what's actually happening currently.
    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert field_to_remove in str(e.value)
    assert_no_success_and_one_failure(spy_notifier)

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
        s3_client, expected_files, "processing/"
    )
    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_manifest_fails_when_invalid_json(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_service_in_utils,
    mock_upload_service,
    spy_notifier,
):
    """Test failure when manifest file is not a valid JSON."""
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(include=True, content="this is not json")
    )

    # Differs to metadata as metadata loads it using dpypelines/pipeline/validate_pipeline.read_json_file method
    # But the manifest uses the implementation in LocalDirectoryStore
    # Implementations should be amended to match
    with pytest.raises(JSONDecodeError):
        start(zip_file_object_key)

    assert_no_success_and_one_failure(spy_notifier)

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
        s3_client, expected_files, "processing/"
    )

    # Not desired behaviour - shouldn't send exception email to data publisher.
    assert_no_emails_sent()
