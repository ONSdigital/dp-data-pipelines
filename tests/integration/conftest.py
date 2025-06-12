import json
import os
import random
import re
import string
import sys
from typing import Optional
import zipfile
import boto3
import pytest
from moto import mock_aws
from unittest.mock import MagicMock, patch
from dpypelines.pipeline.messages.notification import PipelineNotifier
from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
    create_test_zip_file,
)
from tests.integration.constants import dataset_api_url

from tests.integration.mocks.mock_api_responses import MockAPIResponses
from testcontainers.mongodb import MongoDbContainer
import responses

from tests.integration.mocks.mock_db_operations import MockDBOperations


@pytest.fixture(scope="function")
def reset_pipelines_module():
    for key in list(sys.modules.keys()):
        if key.startswith("dpypelines"):
            del sys.modules[key]


TESTING_ENVIRONMENT = "test"
SECRET_ID = f"dp-{TESTING_ENVIRONMENT}-pipeline-secrets"
DEFAULT_ENV_VARS = {
    "ENVIRONMENT": TESTING_ENVIRONMENT,
    "DISABLE_NOTIFICATIONS": "False",
    "DISABLE_EMAILS": "False",
    "COMMIT_SHA": "some-git-commit",
    "DATASET_API_URL": "http://test-dataset-api.url",
    "UPLOAD_SERVICE_URL": "http://test-upload-service.url/upload-new",
    "DE_SLACK_WEBHOOK": "http://test-slack-webhook.url",
    "SERVICE_TOKEN_FOR_UPLOAD": "test-service-token",
    "SES_EMAIL_IDENTITY": "test@example.com",
    "LAMBDA_FAILURE_SLACK_WEBHOOK": "http://test-lambda-webhook.url",
}


@pytest.fixture(scope="function")
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
        yield boto3.client("s3", region_name="eu-west-2")  # type:ignore


@pytest.fixture(scope="function")
def secretsmanager_mock(aws_credentials):
    """Fixture to set up moto Secrets Manager mock."""
    with mock_aws():
        yield boto3.client("secretsmanager")  # type:ignore


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
        ses_client = boto3.client("ses", region_name="eu-west-2")  # type:ignore
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
        "DATABASE_CONNECTION_STRING": "mongodb://test_document_db_connection_string",
        "DATABASE_NAME": "statuses",
    }

    secretsmanager_mock.create_secret(
        Name=SECRET_ID, SecretString=json.dumps(secret_value)
    )
    yield secretsmanager_mock


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
        dataset_id: Optional[str] = None,
    ):
        bucket_name = "test-pipeline-bucket"
        if dataset_id:
            zip_key = f"input/{dataset_id}.zip"
        else:
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

        # Get data file size for Upload Service request parameters
        if data_config.include:
            data_file_size = zipfile.ZipFile(zip_file).getinfo(data_file_name).file_size
        else:
            data_file_size = 0

        # Upload the zip file
        with open(zip_file, "rb") as f:
            s3_mock.put_object(Bucket=bucket_name, Key=zip_key, Body=f.read())
        return f"{bucket_name}/{zip_key}", data_file_size

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


@pytest.fixture(scope="function")
def mock_api_responses(aws_credentials, responses):
    """Pytest fixture that provides a configured MockDatasetApi instance"""
    api_mock = MockAPIResponses(responses)
    yield api_mock
    api_mock.remove_all_mocks()


@pytest.fixture(scope="function")
def mock_upload_service(aws_credentials):
    with patch(
        "dpypelines.pipeline.api.upload.UploadServiceClient"
    ) as mock_upload_service:
        mock = MagicMock()
        mock_upload_service.return_value = mock
        yield mock


@pytest.fixture(scope="function")
def spy_notifier(mocker, reset_pipelines_module, aws_credentials, mock_slack):
    # Save the original class before patching
    original_notifier_class = PipelineNotifier

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

    with patch("dpypelines.pipeline.messages.notification.PipelineNotifier") as e:
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


@pytest.fixture(scope="function")
def mock_job_config():
    with patch("dpypelines.pipeline.config.get_job_config") as mock_get_job_config:
        mock_config = MagicMock()
        mock_get_job_config.return_value = mock_get_job_config
        yield mock_config


mongo = MongoDbContainer("mongo:7.0.7")


@responses.activate
@pytest.fixture(scope="session", autouse=True)
def setup_mongodb(request):
    responses.add_passthru(prefix=re.compile(pattern=r"http\+docker://"))
    mongo.start()

    def remove_container():
        responses.add_passthru(prefix=re.compile(pattern=r"http\+docker://"))

        mongo.stop()

    request.addfinalizer(remove_container)

    os.environ["DATABASE_CONNECTION_STRING"] = mongo.get_connection_url()
    os.environ["DATABASE_NAME"] = mongo.dbname
    return mongo


@pytest.fixture(scope="function")
def mock_db_operations():
    db_mock = MockDBOperations(mongo)
    yield db_mock
    db_mock.delete_data()
