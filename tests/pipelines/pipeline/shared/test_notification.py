import pytest


def test_notify_se_webook_raises_for_missing_env_var(monkeypatch):
    """
    Test that when we import notification, an assertion error
    is correctly raised if we're missing the env var
    specifying the software engineering slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    monkeypatch.setenv("PS_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("DE_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("DS_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from dpypelines.pipeline.shared import notification

        client = notification.SEMessenger()

    assert "Unable to find required environment variable SE_SLACK_WEBHOOK" in str(
        e.value
    )


def test_notify_ps_webook_raises_for_missing_env_var(monkeypatch):
    """
    Test that when we import notification, an assertion error
    is correctly raised if we're missing the env var
    specifying the publishing support slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    monkeypatch.setenv("SE_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("DE_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("DS_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from dpypelines.pipeline.shared import notification

        client = notification.PSMessenger()

    assert "Unable to find required environment variable PS_SLACK_WEBHOOK" in str(
        e.value
    )


def test_notify_de_webook_raises_for_missing_env_var(monkeypatch):
    """
    Test that when we import notification, an assertion error
    is correctly raised if we're missing the env var
    specifying the data engineering slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    monkeypatch.setenv("PS_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("SE_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("DS_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from dpypelines.pipeline.shared import notification

        client = notification.DEMessenger()

    assert "Unable to find required environment variable DE_SLACK_WEBHOOK" in str(
        e.value
    )


def test_notify_ds_webook_raises_for_missing_env_var(monkeypatch):
    """
    Test that when we import notification, an assertion error
    is correctly raised if we're missing the env var
    specifying the data submmitter of validation error slack webhook
    """

    # use monkeypatch to set all the env vars other than
    # the one you're testing for the absense of
    monkeypatch.setenv("PS_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("DE_SLACK_WEBHOOK", "not-used")
    monkeypatch.setenv("SE_SLACK_WEBHOOK", "not-used")

    with pytest.raises(AssertionError) as e:
        from dpypelines.pipeline.shared import notification

        client = notification.DSMessenger()

    assert "Unable to find required environment variable DS_SLACK_WEBHOOK" in str(
        e.value
    )
