import os
import re
import sys
import pytest
import responses
from dpypelines.pipeline.config.job_config import get_job_config
from tests.e2e.e2e_test_base import E2ETestBase

ENVIRONMENT = os.environ.get("ENVIRONMENT", "sandbox")
SECRET_ID = f"dp-{ENVIRONMENT}-pipeline-secrets"

DEFAULT_ENV_VARS = {
    "ENVIRONMENT": ENVIRONMENT,
    "DISABLE_NOTIFICATIONS": "False",
    "DISABLE_EMAILS": "False",
    "COMMIT_SHA": "some-git-commit",
}


@pytest.fixture
def reset_pipelines_module():
    for key in list(sys.modules.keys()):
        if key.startswith("dpypelines"):
            del sys.modules[key]


@pytest.fixture
def configure_env_vars(monkeypatch, reset_pipelines_module):
    def _configure(**kwargs):
        # Set all environment variables
        for key, value in DEFAULT_ENV_VARS.items():
            if value is not None:
                monkeypatch.setenv(key, value)

    return _configure


@pytest.fixture()
def test_base(
    configure_env_vars,
):
    configure_env_vars()

    test_email_address = os.environ[
        "TEST_EMAIL_ADDRESS"
    ]  # Where to send emails to; set as submission contact
    test_base = E2ETestBase(get_job_config(), test_email_address)
    os.environ["SERVICE_TOKEN_FOR_UPLOAD"] = (
        test_base.job_config.service_token_for_upload
    )
    responses.add_passthru(re.compile("http"))
    return test_base
