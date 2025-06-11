from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.messages.setup import get_notifier, setup_clients


@patch("dpypelines.pipeline.messages.setup.get_notifier")
@patch("dpypelines.pipeline.messages.setup.get_email_client")
def test_setup_clients(mock_get_email_client, mock_get_notifier):
    """Test that `setup_clients()` returns the expected clients."""
    mock_notifier = MagicMock()
    mock_email_client = MagicMock()
    mock_get_notifier.return_value = mock_notifier
    mock_get_email_client.return_value = mock_email_client
    mock_job_config = MagicMock()

    notifier, email_client = setup_clients(mock_job_config)

    assert notifier == mock_notifier
    assert email_client == mock_email_client


@patch("dpypelines.pipeline.messages.setup.logger")
@patch("dpypelines.pipeline.messages.setup.create_notifier")
@patch("dpypelines.pipeline.messages.setup.get_local_time")
def test_get_notifier_success(mock_get_local_time, mock_create_notifier, mock_logger):
    """Test successful notifier creation."""
    # Arrange
    mock_config = MagicMock()
    mock_config.de_slack_webhook = "https://hooks.slack.com/test-webhook"
    mock_config.disable_notifications = False

    mock_time = datetime(2024, 1, 1, 12, 0, 0)
    mock_get_local_time.return_value = mock_time

    mock_notifier = MagicMock()
    mock_create_notifier.return_value = mock_notifier

    result = get_notifier(mock_config)

    assert result == mock_notifier

    mock_get_local_time.assert_called_once()

    mock_create_notifier.assert_called_once_with(
        mock_config.de_slack_webhook,
        process_start_time=mock_time,
        disable_notifications=mock_config.disable_notifications,
    )

    mock_logger.info.assert_called_once_with(
        "Notifier created", data={"notifier": mock_notifier}
    )

    mock_logger.error.assert_not_called()


@patch("dpypelines.pipeline.messages.setup.logger")
@patch("dpypelines.pipeline.messages.setup.create_notifier")
@patch("dpypelines.pipeline.messages.setup.get_local_time")
def test_get_notifier_create_notifier_raises_exception(
    mock_get_local_time, mock_create_notifier, mock_logger
):
    """Test exception raised + logged when notifier creation errors"""
    mock_config = MagicMock()
    mock_config.de_slack_webhook = "https://hooks.slack.com/test-webhook"
    mock_config.disable_notifications = False

    mock_time = datetime(2024, 1, 1, 12, 0, 0)
    mock_get_local_time.return_value = mock_time

    test_exception = ValueError("Invalid webhook URL")
    mock_create_notifier.side_effect = test_exception

    with pytest.raises(ValueError, match="Invalid webhook URL"):
        get_notifier(mock_config)

    mock_logger.error.assert_called_once_with(
        "Error occurred when creating notifier", error=test_exception
    )

    mock_logger.info.assert_not_called()
