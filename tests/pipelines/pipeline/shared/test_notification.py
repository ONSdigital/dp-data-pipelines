from unittest.mock import MagicMock

import pytest
from _pytest.monkeypatch import MonkeyPatch

from dpypelines.pipeline.shared.notification import (
    NopNotifier,
    PipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.utils import get_commit_id


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


def test_notification_custom_postfix_success():
    """
    Test that we can add a custom postfix to the notification message
    for a success.
    """
    postfix_str = "i-might-be-a-url"

    mp = MonkeyPatch()
    mp.setenv("DISABLE_NOTIFICATIONS", "False")
    mp.setenv("NOTIFICATION_POSTFIX", postfix_str)

    notifier = PipelineNotifier("_")
    notifier.client = MagicMock()
    notifier.success()

    notifier.client.msg_str.assert_called_once()
    assert (
        f":white_check_mark: {postfix_str}, commit ID: {get_commit_id()}, source ID: {None}, processing start time: {None}, processing end time:"
        in notifier.client.msg_str._calls_repr()
    )
    assert "environment:" in notifier.client.msg_str._calls_repr()


def test_notification_custom_postfix_failure():
    """
    Test that we can add a custom postfix to the notification message
    for a failure.
    """
    postfix_str = "i-might-be-a-url"

    mp = MonkeyPatch()
    mp.setenv("DISABLE_NOTIFICATIONS", "False")
    mp.setenv("NOTIFICATION_POSTFIX", postfix_str)

    notifier = PipelineNotifier("_")
    notifier.client = MagicMock()
    notifier.failure()

    notifier.client.msg_str.assert_called_once()
    assert (
        f":x: {postfix_str}, commit ID: {get_commit_id()}, source ID: {None}, processing start time: {None}, processing end time:"
        in notifier.client.msg_str._calls_repr()
    )
    assert "environment:" in notifier.client.msg_str._calls_repr()
