from unittest.mock import MagicMock, patch
from importlib import reload

import dpypelines.pipeline.config.job_configuration
from dpypelines.pipeline.config.secret_config import SecretConfig

secrets = {}

expected_secret_name = "dp-sandbox-secrets"
dataset_api_url = SecretConfig("DATASET_API_URL", "dataset_api_url")
upload_service_url = SecretConfig("UPLOAD_SERVICE_URL", "upload_service_url")
de_slack_webhook = SecretConfig("DE_SLACK_WEBHOOK", "de_slack_webhook")
service_token_for_upload = SecretConfig(
    "SERVICE_TOKEN_FOR_UPLOAD", "service_token_for_upload"
)
ses_email_identity = SecretConfig("SES_EMAIL_IDENTITY", "ses_email_identity")
lambda_failure_slack_webhook = SecretConfig(
    "LAMBDA_FAILURE_SLACK_WEBHOOK", "lambda_failure_slack_webhook"
)
secrets_config = (
    dpypelines.pipeline.config.job_configuration.get_default_secrets_config()
)
test_secret_config = [
    dataset_api_url,
    upload_service_url,
    de_slack_webhook,
    service_token_for_upload,
    ses_email_identity,
    lambda_failure_slack_webhook,
]


def create_test_secret_value(config: SecretConfig):
    if config.mappings is None:
        return f"{secret.secret_id} - {secret.config_attribute}"

    value = {}

    for mapping in config.mappings:
        value[mapping.secret_key] = f"{mapping.config_attribute} - {mapping.secret_key}"

    return value


for secret in test_secret_config:
    secrets[secret.secret_id] = create_test_secret_value(secret)


def get_secret(secret_id: str):
    mock_secret = MagicMock()

    secret_exists = secret_id in secrets
    if not secret_exists:
        mock_secret.error = f"Could not find secret {secret_id}"
        mock_secret.value = None
        mock_secret.success = False
    else:
        mock_secret.error = None
        mock_secret.value = secrets[secret_id]
        mock_secret.success = True

    return mock_secret


@patch("dpytools.secrets.secrets_client.SecretsClient")
def test_demo(secrets_client):
    secrets_client.return_value.get_secret.side_effect = get_secret
    reload(dpypelines.pipeline.config.job_configuration)
    from dpypelines.pipeline.config.job_configuration import JobConfiguration

    config = JobConfiguration(
        secrets_client=secrets_client(), secrets_config=test_secret_config
    )
    assert config
