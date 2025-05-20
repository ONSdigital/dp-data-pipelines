import json
import sys
from moto import mock_aws
import pytest

from tests.integration.helpers.ses_assertion_helpers import assert_no_emails_sent
from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi


# Environment variables
@mock_aws
def test_invalid_environment_variables(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    monkeypatch,
    reset_pipelines_module,
):
    """
    Test environment variables are a non-boolean string but should be boolean
    """
    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "not-a-bool")

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    with pytest.raises(ValueError) as e:
        start(zip_file_object_key)

    assert "A str value representing a boolean" in str(e)
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
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    reset_pipelines_module,
):
    """
    Test secret not found in AWS Secrets Manager
    """
    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "Failed to retrieve secrets from AWS Secrets Manager" in str(e)
    # Not desired behaviour
    assert_no_emails_sent()


@mock_aws
def test_missing_secret_keys(
    zip_file_object_key_factory,
    secretsmanager_mock,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_dataset_api: MockDatasetApi,
    mock_upload_service,
    spy_notifier,
    reset_pipelines_module,
):
    """
    Secret exists in AWS Secrets Manager, but is missing a key value
    """
    secret_value = {
        "DATASET_API_URL": "http://test-dataset-api.url",
        "UPLOAD_SERVICE_URL": "http://test-upload-service.url",
    }

    secretsmanager_mock.create_secret(
        Name="dp-test-secrets", SecretString=json.dumps(secret_value)
    )

    for key in list(sys.modules.keys()):
        if key.startswith("JobConfiguration") or key.endswith("JobConfiguration"):
            del sys.modules[key]

    from dpypelines.s3_folder_received import start

    zip_file_object_key = zip_file_object_key_factory()

    with pytest.raises(AttributeError) as e:
        start(zip_file_object_key)

    assert "Secret key '" in str(e)
    assert "missing" in str(e)
    # Not desired behaviour
    assert_no_emails_sent()
