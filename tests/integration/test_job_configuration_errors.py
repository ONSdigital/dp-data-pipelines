import json
import os
from moto import mock_aws
from pydantic import ValidationError
import pytest

from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.conftest import SECRET_ID
from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


# Environment variables
@mock_aws
def test_invalid_environment_variables(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    monkeypatch,
    reset_pipelines_module,
):
    """
    Test environment variables are a non-boolean string but should be boolean
    """
    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "not-a-bool")

    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    error_string = str(e)
    assert "Input should be a valid boolean" in str(e)
    assert "DISABLE_NOTIFICATIONS" in error_string

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    assert dataset is None

    mock_api_responses.assert_no_requests()

    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "False")
    # Not desired behaviour
    assert_no_emails_sent()


# Secrets
@mock_aws
def test_missing_secret(
    zip_file_object_key_factory,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    reset_pipelines_module,
):
    """
    Test secret not found in AWS Secrets Manager
    """
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "Secrets Manager can't find the specified secret" in str(e)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    assert dataset is None

    mock_api_responses.assert_no_requests()

    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_missing_secret_keys(
    zip_file_object_key_factory,
    secretsmanager_mock,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    reset_pipelines_module,
    monkeypatch,
):
    """
    Secret exists in AWS Secrets Manager, but is missing a key value
    """
    webhook_value = os.environ["DE_SLACK_WEBHOOK"]
    monkeypatch.delenv("DE_SLACK_WEBHOOK")

    secret_value = {
        "DATASET_API_URL": "http://test-dataset-api.url",
        "UPLOAD_SERVICE_URL": "http://test-upload-service.url",
    }

    secretsmanager_mock.create_secret(
        Name=SECRET_ID, SecretString=json.dumps(secret_value)
    )

    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(ValidationError) as e:
        start(zip_file_object_key)

    assert "1 validation error for JobConfig\nDE_SLACK_WEBHOOK" in str(e)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    assert dataset is None

    mock_api_responses.assert_no_requests()

    monkeypatch.setenv("DE_SLACK_WEBHOOK", webhook_value)
