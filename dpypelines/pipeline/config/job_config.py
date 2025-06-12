import os
from typing import Self
from pydantic import Field, model_validator
from pydantic_settings import (
    AWSSecretsManagerSettingsSource,
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

DEFAULT_ENVIRONMENT = "sandbox"


class JobConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env"), env_parse_enums=True, case_sensitive=False, extra="ignore"
    )

    environment: str = Field(default=DEFAULT_ENVIRONMENT)

    dataset_api_url: str = Field(alias="DATASET_API_URL")
    upload_service_url: str = Field(alias="UPLOAD_SERVICE_URL")
    de_slack_webhook: str = Field(alias="DE_SLACK_WEBHOOK")
    service_token_for_upload: str = Field(alias="SERVICE_TOKEN_FOR_UPLOAD")
    ses_email_identity: str = Field(alias="SES_EMAIL_IDENTITY")
    lambda_failure_slack_webhook: str = Field(alias="LAMBDA_FAILURE_SLACK_WEBHOOK")

    database_connection_string: str = Field(alias="DATABASE_CONNECTION_STRING")
    database_name: str = Field(alias="DATABASE_NAME")

    skip_data_upload: bool = Field(alias="SKIP_DATA_UPLOAD", default=False)
    disable_notifications: bool = Field(alias="DISABLE_NOTIFICATIONS", default=False)
    disable_emails: bool = Field(alias="DISABLE_EMAILS", default=False)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        environment = os.environ.get("ENVIRONMENT", default=DEFAULT_ENVIRONMENT)
        aws_secrets_manager_settings = AWSSecretsManagerSettingsSource(
            settings_cls, secret_id=f"dp-{environment}-pipeline-secrets"
        )
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
            aws_secrets_manager_settings,
        )

    @model_validator(mode="after")
    def export_env_variables(self) -> Self:
        os.environ["SERVICE_TOKEN_FOR_UPLOAD"] = self.service_token_for_upload
        return self


def get_job_config() -> JobConfig:
    return JobConfig.model_validate({})
