import json
import random
import string
import sys
import boto3
import pytest
from moto import mock_aws
from unittest.mock import MagicMock, patch
from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
    create_test_zip_file,
)
from tests.integration.constants import dataset_api_url
import dpypelines.pipeline.utils

from tests.integration.mocks.mock_dataset_api_client import MockDatasetApi


@pytest.fixture
def reset_pipelines_module():
    for key in list(sys.modules.keys()):
        if key.startswith("dpypelines"):
            del sys.modules[key]


DEFAULT_ENV_VARS = {
    "ENVIRONMENT": "test",
    "DISABLE_NOTIFICATIONS": "False",
    "DISABLE_EMAILS": "False",
    "COMMIT_SHA": "some-git-commit",
}


@pytest.fixture
def configure_env_vars(monkeypatch, reset_pipelines_module):
    def _configure(**kwargs):
        # Set default values
        env_vars = DEFAULT_ENV_VARS.copy()

        # Override with any provided values
        env_vars.update(kwargs)

        # Set all environment variables
        for key, value in env_vars.items():
            if value is not None:
                monkeypatch.setenv(key, value)
            else:
                monkeypatch.delenv(key)

    return _configure


@pytest.fixture(scope="function")
def aws_credentials(configure_env_vars, monkeypatch):
    """
    Mocked AWS Credentials for boto3.
    """
    configure_env_vars()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-2")
    monkeypatch.delenv("AWS_PROFILE", None)
    yield

    for key, value in DEFAULT_ENV_VARS.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(scope="function")
def s3_mock(aws_credentials):
    """Fixture to set up moto S3 mock."""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def secretsmanager_mock(aws_credentials):
    """Fixture to set up moto Secrets Manager mock."""
    with mock_aws():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


def email_validation_mock_implementation(email: str):
    mock_validation_result = MagicMock()
    mock_validation_result.normalized = email
    mock_validation_result.original = email
    return mock_validation_result


@pytest.fixture(scope="function")
def ses_client_email_validator_mock(monkeypatch, aws_credentials):
    with patch("dpytools.email.ses.client.validate_email") as mock_email_validator:
        mock_email_validator.side_effect = email_validation_mock_implementation
        yield


@pytest.fixture(scope="function")
def utils_email_validator_mock(monkeypatch, aws_credentials):
    with patch(
        "dpypelines.pipeline.messages.utils.validate_email"
    ) as mock_email_validator:
        mock_email_validator.side_effect = email_validation_mock_implementation
        yield


@pytest.fixture(scope="function")
def ses_mock(aws_credentials, ses_client_email_validator_mock):
    """Fixture to set up moto SES mock."""
    with mock_aws():
        ses_client = boto3.client("ses", region_name="eu-west-2")
        ses_client.verify_email_identity(EmailAddress="test@example.com")
        yield ses_client


@pytest.fixture(scope="function")
def setup_secrets(secretsmanager_mock, aws_credentials):
    """Setup mock secrets in AWS Secrets Manager."""
    secret_value = {
        "DATASET_API_URL": dataset_api_url,
        "UPLOAD_SERVICE_URL": "http://test-upload-service.url",
        "DE_SLACK_WEBHOOK": "http://test-slack-webhook.url",
        "SERVICE_TOKEN_FOR_UPLOAD": "test-service-token",
        "SES_EMAIL_IDENTITY": "test@example.com",
        "LAMBDA_FAILURE_SLACK_WEBHOOK": "http://test-lambda-webhook.url",
    }

    secretsmanager_mock.create_secret(
        Name="dp-test-secrets", SecretString=json.dumps(secret_value)
    )


def generate_random_file_name():
    return "".join(
        random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits)
        for _ in range(16)
    )


valid_file_config = FileGenerationConfig(True, False, False)


@pytest.fixture(scope="function")
def zip_file_object_key_factory(s3_mock, create_zip_file_factory, aws_credentials):
    """Upload the test zip file to the S3 mock."""

    def factory(
        manifest_config: FileGenerationConfig = valid_file_config,
        metadata_config: FileGenerationConfig = valid_file_config,
        data_config: FileGenerationConfig = valid_file_config,
        data_file_name: str = "data.csv",
    ):
        bucket_name = "test-pipeline-bucket"
        zip_key = f"input/{generate_random_file_name()}.zip"

        # Create the S3 bucket
        s3_mock.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        zip_file = create_zip_file_factory(
            manifest_config=manifest_config,
            metadata_config=metadata_config,
            data_config=data_config,
            data_file_name=data_file_name,
        )
        # Upload the zip file
        with open(zip_file, "rb") as f:
            s3_mock.put_object(Bucket=bucket_name, Key=zip_key, Body=f.read())

        return f"{bucket_name}/{zip_key}"

    return factory


@pytest.fixture(scope="function")
def create_zip_file_factory(aws_credentials):
    """Create a temporary zip file with test data for pipeline processing."""
    created_files = []

    def factory(
        manifest_config: FileGenerationConfig,
        metadata_config: FileGenerationConfig,
        data_config: FileGenerationConfig,
        data_file_name: str,
    ):
        zip_path = create_test_zip_file(
            manifest_config=manifest_config,
            metadata_config=metadata_config,
            data_config=data_config,
            data_file_name=data_file_name,
        )
        created_files.append(zip_path)
        return zip_path

    yield factory

    for zip_path in created_files:
        if zip_path.exists():
            zip_path.unlink()


@pytest.fixture
def mock_dataset_api(aws_credentials, responses):
    """Pytest fixture that provides a configured MockDatasetApi instance"""
    api_mock = MockDatasetApi(responses)
    yield api_mock
    api_mock.remove_all_mocks()


@pytest.fixture(scope="function")
def mock_upload_service(aws_credentials):
    with patch("dpypelines.pipeline.utils.UploadServiceClient") as mock_upload_service:
        mock = MagicMock()
        mock_upload_service.return_value = mock
        yield mock


@pytest.fixture(scope="function")
def spy_notifier(mocker, reset_pipelines_module, aws_credentials, mock_slack):
    # Save the original class before patching
    original_notifier_class = dpypelines.pipeline.utils.PipelineNotifier

    # Create a class that inherits from the original but lets us spy on methods
    class SpyPipelineNotifier(original_notifier_class):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.client = mock_slack

            success_mock = MagicMock()
            success_mock.side_effect = super().success
            self.success = success_mock

            failure_mock = MagicMock()
            failure_mock.side_effect = super().failure
            self.failure = failure_mock

    with patch("dpypelines.pipeline.utils.PipelineNotifier") as e:
        instances = []

        def create_notifier(*args, **kwargs):
            instance = SpyPipelineNotifier(*args, **kwargs)
            instances.append(instance)
            return instance

        e.side_effect = create_notifier
        e.instances = instances
        yield e


@pytest.fixture(scope="function")
def mock_slack(aws_credentials):
    """Disable the notifier to avoid real Slack calls."""
    with patch(
        "dpypelines.pipeline.messages.notification.SlackMessenger"
    ) as mock_get_notifier:
        mock_get_notifier.msg_str.return_value = None
        yield mock_get_notifier
