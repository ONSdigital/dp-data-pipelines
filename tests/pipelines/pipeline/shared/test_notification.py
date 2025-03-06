from unittest.mock import MagicMock, patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from dpypelines.pipeline.messages.notification import PipelineNotifier


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

    with patch.dict("os.environ", {"COMMIT_SHA": "123456789abcdef"}):
        notifier = PipelineNotifier("_")
        notifier.client = MagicMock()
        notifier.success()

        notifier.client.msg_str.assert_called_once()
        assert (
            f":white_check_mark: {postfix_str}, commit ID: 123456789abcdef, source ID: {None}, processing start time: {None}, processing end time:"
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

    with patch.dict("os.environ", {"COMMIT_SHA": "123456789abcdef"}):
        notifier = PipelineNotifier("_")
        notifier.client = MagicMock()
        notifier.failure()

        notifier.client.msg_str.assert_called_once()
        assert (
            f":x: {postfix_str}, commit ID: 123456789abcdef, source ID: {None}, processing start time: {None}, processing end time:"
            in notifier.client.msg_str._calls_repr()
        )
        assert "environment:" in notifier.client.msg_str._calls_repr()
