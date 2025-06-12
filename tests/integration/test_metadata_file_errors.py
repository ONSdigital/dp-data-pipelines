from pydantic import ValidationError
import pytest
from moto import mock_aws

from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.file_helpers import (
    METADATA_FILE_NAME,
    FileGenerationConfig,
)
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.ses_assertion_helpers import (
    assert_email_sent,
    assert_successful_email,
)
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_missing_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test pipeline handles missing metadata file"""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(include=False)
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert METADATA_FILE_NAME in str(e)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == "Required file not found: metadata.json"

    assert_no_success_and_one_failure(spy_notifier)
    mock_api_responses.assert_no_requests()


@mock_aws
def test_empty_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test pipeline handles an empty metadata file."""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(include=True, empty=True)
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "File is empty" in str(e)
    assert METADATA_FILE_NAME in str(e)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == f"File is empty: {METADATA_FILE_NAME}"

    assert_no_success_and_one_failure(spy_notifier)
    mock_api_responses.assert_no_requests()

    assert_email_sent(
        [
            "An error has occurred:",
            f"File is empty: {METADATA_FILE_NAME}",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        "ETL Pipeline error has occurred",
    )


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
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    field_to_remove,
):
    """Test a pipeline handles errors when metadata is missing required fields"""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(
            include=True, missing_field_keys=[field_to_remove]
        )
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(ValidationError) as e:
        start(zip_file_object_key)

    assert field_to_remove in str(e.value)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert (
        f"1 validation error for MinimalMetadata\n{field_to_remove}"
        in status["error_message"]
    )

    assert_no_success_and_one_failure(spy_notifier)
    mock_api_responses.assert_no_requests()

    assert_email_sent(
        [
            "An error has occurred:",
            f"1 validation error for MinimalMetadata\n{field_to_remove}",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        "ETL Pipeline error has occurred",
    )


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
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    field_to_remove,
):
    """Test pipeline still works when metadata is missing optional fields"""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(
            include=True, missing_field_keys=[field_to_remove]
        )
    )
    s3_object = S3Object(zip_file_object_key)

    result = start(zip_file_object_key)
    assert result

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "COMPLETED"
    assert len(status["events"]) == 5

    assert len(spy_notifier.instances) == 1
    spy_notifier.instances[0].success.assert_called_once()

    mock_api_responses.assert_all_requests_made()
    assert_successful_email()


@mock_aws
def test_metadata_fails_when_invalid_json(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test a successful pipeline execution with mocked AWS services."""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        metadata_config=FileGenerationConfig(include=True, content="this is not json")
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "not valid JSON" in str(e.value), str(e.value)
    assert "metadata.json" in str(e.value), str(e.value)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert (
        "not valid JSON: Expecting value: line 1 column 1 (char 0)"
        in status["error_message"]
    )

    assert_no_success_and_one_failure(spy_notifier)
    mock_api_responses.assert_no_requests()

    assert_email_sent(
        [
            "An error has occurred:",
            "not valid JSON: Expecting value: line 1 column 1 (char 0)",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        "ETL Pipeline error has occurred",
    )
