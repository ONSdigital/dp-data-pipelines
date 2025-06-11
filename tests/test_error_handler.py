from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.messages.error_handler_module import (
    error_handler,
    send_error_email,
)


@patch("dpypelines.pipeline.messages.error_handler_module.logger")
class TestErrorHandler:
    def test_error_handler_fail(self, mock_logger):
        """Testing that `error handler` raises a `TypeError` when argument/arguments are missing"""
        with pytest.raises(TypeError):
            error_handler(section="1.1")  # type: ignore

    def test_error_handler_success_with_data(self, mock_logger):
        """Testing if all arguments provided the then the function works as intended."""

        mock_email_client = MagicMock()
        mock_notifier = MagicMock()

        with pytest.raises(Exception) as err:
            error_handler(
                section="1.2.1",
                error=Exception("This is a Test Error"),
                data={"TestKey": "Test value"},
                submitter_email="test@gmail.com",
                enable_email=True,
                enable_logs=True,
                enable_notification=True,
                email_client=mock_email_client,
                notifier=mock_notifier,
            )

        assert str(err.value) == "This is a Test Error"

        # Test logger usage
        mock_logger.info.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error",
            data={"TestKey": "Test value"},
        )

        # Test email client usage
        mock_email_client.send.assert_called_once_with(
            "test@gmail.com",
            "ETL Pipeline error has occurred in Section: 1.2.1",
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error\n\n Additional Data: {'TestKey': 'Test value'}",
        )

        # Test notifier usage
        mock_notifier.failure.assert_called_once()

    def test_error_handler_success_without_data(self, mock_logger):
        """Testing if not providing `data` which is optional, the function still works as intended."""
        mock_email_client = MagicMock()
        mock_notifier = MagicMock()

        with pytest.raises(Exception) as err:
            error_handler(
                section="1.2.1",
                error=Exception("This is a Test Error"),
                data=None,
                submitter_email="test@gmail.com",
                enable_email=True,
                enable_logs=True,
                enable_notification=True,
                email_client=mock_email_client,
                notifier=mock_notifier,
            )

        assert str(err.value) == "This is a Test Error"

        # Test logger usage
        mock_logger.info.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error"
        )

        # Test email client usage
        mock_email_client.send.assert_called_once_with(
            "test@gmail.com",
            "ETL Pipeline error has occurred in Section: 1.2.1",
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error",
        )

        # Test failure method to valide error notification was triggered once
        mock_notifier.failure.assert_called_once()

    @pytest.mark.parametrize(
        "submitter_email,enable_email",
        [
            ("", True),
            ("test_email@testing.com", False),
            ("", False),
            ("test_email@testing.com", True),
        ],
    )
    def test_error_handler_does_not_send_email_conditionally(
        self, mock_logger, submitter_email, enable_email
    ):
        """Testing if all arguments provided the then the function works as intended."""

        mock_email_client = MagicMock()
        mock_notifier = MagicMock()

        # For testing when we have both the submitter email, AND enable_email is True; we want to test what happens if email_client is None
        email_client = (
            mock_email_client
            if not submitter_email or not enable_email or submitter_email == ""
            else None
        )
        with pytest.raises(Exception) as err:
            error_handler(
                section="1.2.1",
                error=Exception("This is a Test Error"),
                data={"TestKey": "Test value"},
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=True,
                enable_notification=True,
                email_client=email_client,
                notifier=mock_notifier,
            )

        assert str(err.value) == "This is a Test Error"

        # Test logger usage
        mock_logger.info.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error",
            data={"TestKey": "Test value"},
        )

        # Test email client usage
        mock_email_client.send.assert_not_called()

        # Test notifier usage
        mock_notifier.failure.assert_called_once()

    def test_send_error_email_success(self, mock_logger):
        """Testing if all arguments provided the then the function works as intended."""
        mock_email_client = MagicMock()

        # Call the function with proper arguments
        send_error_email(
            section="1.2.1",
            error="Test email error",
            submitter_email="test@gmail.com",
            data={"extra": "details"},
            email_client=mock_email_client,
        )

        # Validate email client usage
        mock_email_client.send.assert_called_once_with(
            "test@gmail.com",
            "ETL Pipeline error has occurred in Section: 1.2.1",
            "An error has occurred in section: 1.2.1 \n\nTest email error\n\n Additional Data: {'extra': 'details'}",
        )

    def test_send_error_email_failure(self, mock_logger):
        """Testing that when an Exception is triggered the function behaves as expected."""

        # Simulate failure in the send method (also git was complaining that the variable wasn't used)

        mock_email_client = MagicMock()
        mock_email_client.send.side_effect = Exception("Send failure")

        # Call the function with proper arguments
        send_error_email(
            section="2.2",
            error="Test email failure",
            submitter_email="test@gmail.com",
            data={"info": "test"},
            email_client=mock_email_client,
        )

        # Validate that logger.error was called
        mock_logger.error.assert_called_once()

        # Extract the actual call arguments
        log_call_args = mock_logger.error.call_args

        # Validate the first argument (message)
        assert log_call_args[0][0] == "Failed to send error email notification"

        # Validate the second argument (exception)
        assert len(log_call_args) > 1

        error = log_call_args[1]

        if "error" in error:
            error = error["error"]

        assert error is not None
        assert isinstance(error, Exception)
        assert str(error) == "Send failure"

    def test_does_not_send_email_conditionally(self, mock_logger):
        """Testing that when an Exception is triggered the function behaves as expected."""

        # Simulate failure in the send method (also git was complaining that the variable wasn't used)

        mock_email_client = MagicMock()
        mock_email_client.send.side_effect = Exception("Send failure")

        # Call the function with proper arguments
        send_error_email(
            section="2.2",
            error="Test email failure",
            submitter_email="test@gmail.com",
            data={"info": "test"},
            email_client=mock_email_client,
        )

        # Validate that logger.error was called
        mock_logger.error.assert_called_once()

        # Extract the actual call arguments
        log_call_args = mock_logger.error.call_args

        # Validate the first argument (message)
        assert log_call_args[0][0] == "Failed to send error email notification"

        # Validate the second argument (exception)
        assert len(log_call_args) > 1

        error = log_call_args[1]

        if "error" in error:
            error = error["error"]

        assert error is not None
        assert isinstance(error, Exception)
        assert str(error) == "Send failure"
