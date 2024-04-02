import pytest


def test_notification_raises_for_missing_webhook_env_var():
    """
    Test that when we import notification, an assertion error
    is correctly raised if we're missing the env var
    specifying the slack webhook
    """

    with pytest.raises(AssertionError) as e:
        from dpypelines.pipeline.shared import notification

        client = notification.PipelineMessenger(None)

    assert (
        "Unable to find required environment variable to populate webhook_url argument"
        in str(e.value)
    )
