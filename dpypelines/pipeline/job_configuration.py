import os
from typing import List, Optional

from dpytools.secrets.secrets_client import SecretsClient
from dpytools.utilities.utilities import str_to_bool

"""
Secrets to retrieve from AWS Secrets Manager
"""
secrets_config = [
    # Secret ID, JobConfiguration attribute
    ("DATASET_API_URL", "dataset_api_url"),
    ("UPLOAD_SERVICE_URL", "upload_service_url"),
    ("DE_SLACK_WEBHOOK", "de_slack_webhook"),
    ("SERVICE_TOKEN_FOR_UPLOAD", "service_token_for_upload"),
    ("SES_EMAIL_IDENTITY", "ses_email_identity"),
    ("LAMBDA_FAILURE_SLACK_WEBHOOK", "lambda_failure_slack_webhook"),
]

"""
Environment variables to retrieve
"""
environment_variables_config = [
    # Environment var name, JobConfiguration attribute, default value
    ("SKIP_DATA_UPLOAD", "skip_data_upload", False),
    ("DISABLE_NOTIFICATIONS", "disable_notifications", False),
    ("DISABLE_EMAILS", "disable_emails", False),
]


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
            cls._instance = super(JobConfiguration, cls).__new__(cls)

        return cls._instance

    def __init__(
        self,
        secrets_config: List[tuple] = secrets_config,
        environment_config: List[tuple] = environment_variables_config,
        secrets_client: Optional[SecretsClient] = None,
    ):
        if secrets_client is None:
            secrets_client = SecretsClient()

        self.secrets_client = secrets_client
        self.secrets_config = secrets_config
        self.environment_config = environment_config

    def load_config(self) -> Optional[str]:
        """
        Load config variables

        :return: Error message (if any)
        """
        error = self._load_secrets()
        if error is not None:
            self.error = error
            self.loaded = True
            return error

        self._load_env_vars()
        self.loaded = True

    def _load_env_vars(self):
        """
        Loads config variables from environment vars
        """
        for name, attribute, default_value in self.environment_config:
            self._load_environment_variable(name, attribute, default_value)

    def _load_secrets(self) -> Optional[str]:
        """
        Loads config variables from AWS Secrets

        :return: Error message if any
        """
        for secret_config in self.secrets_config:
            error = self._load_secret(secret_config[0], secret_config[1])

            if error is not None:
                return error

        return None

    def _load_secret(self, secret_name: str, class_attribute: str) -> Optional[str]:
        """
        Load a secret from AWS Secrets Manager and set the appropriate attribute of the
        JobConfiguration instance.

        :param secret_name: Secret name/ID to retrieve
        :param class_attribute: Attribute of JobConfiguration that the secret should be set to

        :return: Error message if any
        """
        response = self.secrets_client.get_secret(secret_name)
        if response.success and response.value is not None:
            self.__setattr__(class_attribute, response.value)

        return response.error

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
