import pytest
from _pytest.monkeypatch import MonkeyPatch

from dpypelines.pipeline.shared.notification import (
    PipelineNotifier,
    NopNotifier,
    notifier_from_env_var_webhook
)

def test_notification_constructor():
    """
    Test that the PipelineNotifier can be successfully created from
    a webhook supplied from an env var
    """

    mp = MonkeyPatch()
    fake_web_hook = "fake-web-hook"
    mp.setenv("DISABLE_NOTIFICATIONS", "False")
    mp.setenv("SOME_ENV_VAR", fake_web_hook)

    norifier = notifier_from_env_var_webhook("SOME_ENV_VAR")
    assert isinstance(norifier, PipelineNotifier)
    assert norifier.client.webhook_url == fake_web_hook


def test_notification_constructor_with_disabled_notifications():
    """
    Tests that a NopNotifier is constructed where DISABLE_NOTIFICATIONS
    is true
    """

    mp = MonkeyPatch()
    mp.setenv("DISABLE_NOTIFICATIONS", "True")

    norifier = notifier_from_env_var_webhook("DOES_NOT_MATTER")
    assert isinstance(norifier, NopNotifier)


def test_notification_raises_for_missing_webhook():
    """
    Test that when we import notification, an assertion error
    is correctly raised if we're missing the web hook.
    """

    mp = MonkeyPatch()
    mp.setenv("DISABLE_NOTIFICATIONS", "False")

    with pytest.raises(AssertionError) as e:
        PipelineNotifier(None)

    assert (
        "Unable to find required environment variable to populate webhook_url argument"
        in str(e.value)
    )
