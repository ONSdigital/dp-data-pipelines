import os
from typing import List, Optional

from dpytools.logging.logger import DpLogger
from dpytools.secrets.secret import Secret
from dpytools.secrets.secrets_client import SecretsClient
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.config.secret_config import SecretConfig
from dpypelines.pipeline.config.secret_mapping import SecretMapping


def get_secret_name() -> str:
    return f"dp-{os.environ.get('ENVIRONMENT', 'sandbox')}-secrets"


def get_default_environment_variables_config():
    """
    Environment variables to retrieve
    """
    return [
        # Environment var name, JobConfiguration attribute, default value
        ("SKIP_DATA_UPLOAD", "skip_data_upload", False),
        ("DISABLE_NOTIFICATIONS", "disable_notifications", False),
        ("DISABLE_EMAILS", "disable_emails", False),
    ]


def get_default_secrets_config():
    """
    Secrets to retrieve from AWS Secrets Manager
    """
    return [
        SecretConfig(
            secret_id=get_secret_name(),
            mappings=[
                SecretMapping("DATASET_API_URL", "dataset_api_url"),
                SecretMapping("UPLOAD_SERVICE_URL", "upload_service_url"),
                SecretMapping("DE_SLACK_WEBHOOK", "de_slack_webhook"),
                SecretMapping("SERVICE_TOKEN_FOR_UPLOAD", "service_token_for_upload"),
                SecretMapping("SES_EMAIL_IDENTITY", "ses_email_identity"),
                SecretMapping(
                    "LAMBDA_FAILURE_SLACK_WEBHOOK", "lambda_failure_slack_webhook"
                ),
            ],
        )
    ]


logger = DpLogger("data-ingress-pipeline")


class JobConfiguration:
    """
    Retrieve and store configuration settings for the Glue job.
    """

    loaded: bool = False
    error: Optional[str] = None

    dataset_api_url: Optional[str] = None
    upload_service_url: Optional[str] = None
    de_slack_webhook: Optional[str] = None
    service_token_for_upload: Optional[str] = None
    ses_email_identity: Optional[str] = None
    lambda_failure_slack_webhook: Optional[str] = None

    skip_data_upload: Optional[bool] = None
    disable_notifications: Optional[bool] = None
    disable_emails: Optional[bool] = None

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = object.__new__(cls)

        return cls._instance

    def __init__(
        self,
        secrets_config: Optional[List[tuple]] = None,
        environment_config: Optional[List[tuple]] = None,
        secrets_client: Optional[SecretsClient] = None,
    ):
        self.secrets_client = (
            secrets_client if secrets_client is not None else SecretsClient()
        )
        self.secrets_config = (
            secrets_config
            if secrets_config is not None
            else get_default_secrets_config()
        )
        self.environment_config = (
            environment_config
            if environment_config is not None
            else get_default_environment_variables_config()
        )
        error = self.load_config()

        if error is not None:
            raise Exception(
                f"Failed to retrieve secrets from AWS Secrets Manager. Error: {error}"
            )

    def load_config(self, reload: bool = False) -> Optional[str]:
        """
        Load config variables

        Args:
            reload: Force reloading of config
        :return: Error message (if any)
        """
        if self.loaded and not reload:
            return

        logger.info("Loading JobConfiguration config")

        error = self._load_secrets()
        if error is not None:
            self.error = error
            self.loaded = True
            logger.error(f"Error loading secrets: {self.error}", error=Exception(error))
            return error

        self._load_env_vars()

        self.export_env_vars()
        self.loaded = True
        logger.info("Loaded JobConfiguration config")

    def export_env_vars(self):
        if self.service_token_for_upload:
            os.environ["SERVICE_TOKEN_FOR_UPLOAD"] = self.service_token_for_upload

    def _load_env_vars(self):
        """
        Loads config variables from environment vars
        """
        logger.info("Setting JobConfiguration values from environment variables")
        for name, attribute, default_value in self.environment_config:
            self._load_environment_variable(name, attribute, default_value)

    def _load_secrets(self) -> Optional[str]:
        """
        Loads config variables from AWS Secrets

        :return: Error message if any
        """
        logger.info("Setting JobConfiguration values from secrets")
        for secret_config in self.secrets_config:
            error = self._load_secret(secret_config)

            if error is not None:
                return error

        return None

    def _set_values_from_secret(
        self, secret: Secret, secret_mapping: List[SecretMapping]
    ) -> Optional[str]:
        """
        Set various attributes based on the value of the secret
        :return: Error message if any
        """
        if secret.value is None or not isinstance(secret.value, dict):
            raise ValueError("Expected secret to be a dictionary but it is not.")

        for mapping in secret_mapping:
            value = secret.value.get(mapping.secret_key, None)
            if value is None:
                raise AttributeError(
                    f"Secret key '{mapping.secret_key}' missing in Secret {secret.id}"
                )
            self.__setattr__(mapping.config_attribute, value)

    def _load_secret(self, secret_config: SecretConfig) -> Optional[str]:
        """
        Load a secret from AWS Secrets Manager and set the appropriate attribute of the
        JobConfiguration instance.

        :param secret_name: Secret name/ID to retrieve
        :param class_attribute: Attribute of JobConfiguration that the secret should be set to

        :return: Error message if any
        """
        response = self.secrets_client.get_secret(secret_config.secret_id)
        if (
            response is None
            or not response.success
            or response.error is not None
            or response.value is None
        ):
            return (
                response.error
                if response.error is not None
                else f"An unknown error occurred retrieving {secret_config.secret_id}"
            )

        if secret_config.config_attribute is not None:
            self.__setattr__(secret_config.config_attribute, response.value)

        if secret_config.mappings is not None:
            self._set_values_from_secret(response, secret_config.mappings)

    def _load_environment_variable(
        self, variable_name: str, class_attribute: str, default_value: Optional[str]
    ):
        """
        Load a variable from the OS env settings and set the appropriate attribute of the
        JobConfiguration instance.

        :param variable_name: OS environment variable key
        :param class_attribute: Attribute of JobConfiguration that the secret should be set to
        """
        default = default_value
        if default is not None:
            default = default_value.__str__()

        value = os.environ.get(variable_name, default)

        if default is not None:
            if isinstance(default_value, bool):
                value = str_to_bool(value)

        self.__setattr__(class_attribute, value)
