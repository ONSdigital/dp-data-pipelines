from unittest.mock import patch

import pytest

from dpypelines.pipeline.shared.error_handler_module import (
    error_handler,
    send_error_email,
)


@patch("dpypelines.pipeline.shared.error_handler_module.logger")
@patch("dpypelines.pipeline.shared.error_handler_module.get_email_client")
@patch("dpypelines.pipeline.shared.error_handler_module.get_notifier")
class TestErrorHandler:

    def test_error_handler_fail(self, mock_notifier, mock_email_client, mock_logger):
        """Testing that `error handler` raises a `TypeError` when argument/arguments are missing"""
        print(mock_logger, mock_email_client, mock_notifier)
        with pytest.raises(TypeError):
            error_handler(section="1.1")

    def test_error_handler_success_with_data(
        self, mock_notifier, mock_email_client, mock_logger
    ):
        """Testing if all arguments provided the then the function works as intended."""

        error_handler(
            section="1.2.1",
            error="This is a Test Error",
            data={"TestKey": "Test value"},
            submitter_email="test@gmail.com",
            enable_email=True,
            enable_logs=True,
            enable_notification=True,
        )

        # Test logger usage
        mock_logger.error.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error",
            data={"TestKey": "Test value"},
        )

        # Test email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            "test@gmail.com",
            "ETL Pipeline error has occurred in Section: 1.2.1",
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error\n\n Additional Data: {'TestKey': 'Test value'}",
        )

        # Test notifier usage
        mock_notifier.return_value.failure.assert_called_once()

    def test_error_handler_success_without_data(
        self, mock_notifier, mock_email_client, mock_logger
    ):
        """Testing if not providing `data` which is optional, the function still works as intended."""

        error_handler(
            section="1.2.1",
            error="This is a Test Error",
            data=None,
            submitter_email="test@gmail.com",
            enable_email=True,
            enable_logs=True,
            enable_notification=True,
        )

        # Test logger usage
        mock_logger.error.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error"
        )

        # Test email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            "test@gmail.com",
            "ETL Pipeline error has occurred in Section: 1.2.1",
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error",
        )

        # Test failure method to valide error notification was triggered once
        mock_notifier.return_value.failure.assert_called_once()

    def test_send_error_email_success(
        self, mock_notifier, mock_email_client, mock_logger
    ):
        """Testing if all arguments provided the then the function works as intended."""

        # Call the function with proper arguments
        send_error_email(
            section="1.2.1",
            error="Test email error",
            submitter_email="test@gmail.com",
            data={"extra": "details"},
        )

        # Validate email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            "test@gmail.com",
            "ETL Pipeline error has occurred in Section: 1.2.1",
            "An error has occurred in section: 1.2.1 \n\nTest email error\n\n Additional Data: {'extra': 'details'}",
        )

    def test_send_error_email_failure(
        self, mock_notifier, mock_email_client, mock_logger
    ):
        """Testing that when an Exception is triggered the function behaves as expected."""

        # Simulate failure in the send method (also git was complaining that the variable wasn't used)
        mock_email_client.return_value.send.side_effect = Exception("Send failure")

        # Call the function with proper arguments
        send_error_email(
            section="2.2",
            error="Test email failure",
            submitter_email="test@gmail.com",
            data={"info": "test"},
        )

        # Validate that logger.error was called
        mock_logger.error.assert_called_once()

        # Extract the actual call arguments
        log_call_args = mock_logger.error.call_args

        # Validate the first argument (message)
        assert log_call_args[0][0] == "Failed to send error email notification"

        # Validate the second argument (exception)
        assert isinstance(log_call_args[0][1], Exception)
        assert str(log_call_args[0][1]) == "Send failure"
