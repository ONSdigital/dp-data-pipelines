from json import JSONDecodeError
import pytest
from moto import mock_aws

from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
)
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_missing_manifest(
    setup_mongodb,
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """
    Manifest file missing from zip file
    """
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(include=False)
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(FileNotFoundError) as e:
        start(zip_file_object_key)

    assert "Failed to retrieve manifest from the local directory store" in str(e.value)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert (
        status["error_message"]
        == "Failed to retrieve manifest from the local directory store."
    )

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_no_requests()

    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_empty_manifest(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """
    Manifest file empty.
    """
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(include=True, empty=True)
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(JSONDecodeError):
        start(zip_file_object_key)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == "Expecting value: line 1 column 1 (char 0)"
    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_no_requests()

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
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    field_to_remove,
):
    """
    Manifest file missing fields.
    """
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(
            include=True, missing_field_keys=[field_to_remove]
        )
    )
    s3_object = S3Object(zip_file_object_key)

    # This is the wrong expected behaviour, but it's what's actually happening currently.
    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert field_to_remove in str(e.value)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert (
        f"Manifest schema validation failed: \nException: Invalid manifest\nException details: '{field_to_remove}' is a required property"
        in status["error_message"]
    )

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_no_requests()

    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_manifest_fails_when_invalid_json(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """Test failure when manifest file is not a valid JSON."""
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(include=True, content="this is not json")
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(JSONDecodeError):
        start(zip_file_object_key)

    #    Check database operations
    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == "Expecting value: line 1 column 1 (char 0)"

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_no_requests()

    # Not desired behaviour - shouldn't send exception email to data publisher.
    assert_no_emails_sent()
