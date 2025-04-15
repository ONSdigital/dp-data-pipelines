from importlib import reload
from unittest.mock import MagicMock, patch

import pytest

import dpypelines.pipeline.config.job_configuration
from dpypelines.pipeline.config import SecretConfig
from dpypelines.pipeline.config.job_configuration import secrets_config

secrets = {}

expected_secret_name = "dp-sandbox-secrets"
test_secret_one = SecretConfig("TEST_ONE", "test_attr_one")
test_secret_two = SecretConfig("OTHER_TEST", "other_test_attr")
test_secret_config = [*secrets_config, test_secret_one, test_secret_two]


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
def test_should_load_config(secrets_client):
    """
    Tests that a secrets client can successfully be loaded and a job configuration instance
    can be created taking a secrets client, environment variables config, and secrets config.
    The result should raisei no errors, with the contents matching the expected results.
    """
    secrets_client.return_value.get_secret.side_effect = get_secret

    reload(dpypelines.pipeline.config.job_configuration)
    from dpypelines.pipeline.config.job_configuration import JobConfiguration

    mp = pytest.MonkeyPatch()

    mp.setenv("SKIP_DATA_UPLOAD", "True")
    mp.setenv("DISABLE_NOTIFICATIONS", "True")
    mp.setenv("DISABLE_EMAILS", "True")

    test_environment_vars_config = [
        # Environment var name, JobConfiguration attribute, default value
        ("SKIP_DATA_UPLOAD", "skip_data_upload", True),
        ("DISABLE_NOTIFICATIONS", "disable_notifications", True),
        ("DISABLE_EMAILS", "disable_emails", True),
    ]

    config = JobConfiguration(
        secrets_config=test_secret_config,
        environment_config=test_environment_vars_config,
        secrets_client=secrets_client(),
    )

    assert config.loaded
    assert config.error is None

    for secret_config in secrets_config:
        secret = secrets[secret_config.secret_id]
        if secret_config.config_attribute is not None:
            assert config.__getattribute__(secret_config.config_attribute) == secret
        else:
            for mapping in secret_config.mappings:
                assert (
                    config.__getattribute__(mapping.config_attribute)
                    == secret[mapping.secret_key]
                )

    assert config.disable_emails
    assert config.disable_notifications
    assert config.skip_data_upload


@patch("dpytools.secrets.secrets_client.SecretsClient")
def test_should_load_secrets_from_constructor(secrets_client):
    """
    Tests that a secrets job configuration can load secrets when given a secrets client
    and config as input, and the results are as expected.
    """
    secrets_client.return_value.get_secret.side_effect = get_secret

    reload(dpypelines.pipeline.config.job_configuration)
    from dpypelines.pipeline.config.job_configuration import JobConfiguration

    config = JobConfiguration(
        secrets_client=secrets_client(), secrets_config=test_secret_config
    )
    error = config.load_config()

    assert error is None
    assert config.loaded
    assert config.error is None

    assert config.test_attr_one == secrets[test_secret_one.secret_id]
    assert config.other_test_attr == secrets[test_secret_two.secret_id]


@patch("dpytools.secrets.secrets_client.SecretsClient")
def test_should_load_env_vars_from_constructor(secrets_client):
    """
    Tests that a job configuration can be created taking an environment
    variables config as input, with the results raising no errors and
    matching expected results.
    """
    secrets_client.return_value.get_secret.side_effect = get_secret

    reload(dpypelines.pipeline.config.job_configuration)
    from dpypelines.pipeline.config.job_configuration import JobConfiguration

    # First tuple element == the environment variable config
    # Second tuple element == value to set environment var to
    # Third == what we expect final result to be
    value_set_and_default_value = (
        ("FIRST_ENV_KEY", "env_attr_one", True),
        "False",
        False,
    )
    value_set_and_no_default_value = (
        ("OTHER_ENV_KEY", "env_attr_two", None),
        "False",
        "False",
    )  # No default value is set so does not know to parse as bool
    value_not_set_with_default_value = (
        ("ENV_KEY", "env_attr_three", "default value"),
        None,
        "default value",
    )
    value_not_set_with_no_default = (
        ("FINAL_ENV_KEY", "env_attr_four", None),
        None,
        None,
    )

    # Just the above
    test_env_vars = [
        value_set_and_default_value,
        value_set_and_no_default_value,
        value_not_set_with_default_value,
        value_not_set_with_no_default,
    ]

    test_env_config = [item[0] for item in test_env_vars]

    mp = pytest.MonkeyPatch()

    for env_config, env_value, expected_value in test_env_vars:
        if env_value is not None:
            mp.setenv(env_config[0], env_value)

    config = JobConfiguration(
        secrets_client=secrets_client(), environment_config=test_env_config
    )
    error = config.load_config()

    assert error is None
    assert config.loaded
    assert config.error is None

    for env_config, env_value, expected_value in test_env_vars:
        assert config.__getattribute__(env_config[1]) == expected_value

    assert config.env_attr_one == value_set_and_default_value[2]
    assert config.env_attr_two == value_set_and_no_default_value[2]
    assert config.env_attr_three == value_not_set_with_default_value[2]
    assert config.env_attr_four == value_not_set_with_no_default[2]


@patch("dpytools.secrets.secrets_client.SecretsClient")
def test_should_exits_on_secret_error(secrets_client, monkeypatch):
    """ """
    secrets_client.return_value.get_secret.side_effect = get_secret

    missing_secret = SecretConfig("THIS_SECRET_DOESNT_EXIST", "attr_should_not_be_set")
    secrets_with_missing = [*secrets_config, missing_secret]

    monkeypatch.setenv("SKIP_DATA_UPLOAD", "True")
    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "True")
    monkeypatch.setenv("DISABLE_EMAILS", "True")

    reload(dpypelines.pipeline.config.job_configuration)
    from dpypelines.pipeline.config.job_configuration import JobConfiguration

    config = JobConfiguration(
        secrets_client=secrets_client(), secrets_config=secrets_with_missing
    )

    assert config.error is not None
    assert config.loaded

    # Should not exist
    with pytest.raises(AttributeError):
        config.__getattribute__(missing_secret.secret_id)

    # Since the missing secret was _last_ in the list, this _should_ exist
    assert config.dataset_api_url is not None

    # But these should be None since they will not have been retrieved:
    assert config.disable_emails is None
    assert config.disable_notifications is None
    assert config.skip_data_upload is None


@patch("dpytools.secrets.secrets_client.SecretsClient")
def test_should_exits_early_on_secret_error(secrets_client):
    """ """
    reload(dpypelines.pipeline.config.job_configuration)
    from dpypelines.pipeline.config.job_configuration import JobConfiguration

    secrets_client.return_value.get_secret.side_effect = get_secret

    missing_secret = SecretConfig("THIS_SECRET_DOESNT_EXIST", "attr_should_not_be_set")
    secrets_with_missing = [
        missing_secret,
        *secrets_config,
    ]

    config = JobConfiguration(
        secrets_client=secrets_client(), secrets_config=secrets_with_missing
    )

    assert config.error is not None
    assert config.loaded

    # Should not exist
    with pytest.raises(AttributeError):
        config.__getattribute__(missing_secret.secret_id)

    # Since the missing secret was _first_ in the list, this _should not_ exist as we should have already exited
    assert config.dataset_api_url is None
