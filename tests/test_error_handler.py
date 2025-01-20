from unittest.mock import patch, MagicMock

import pytest

from dpypelines.pipeline.shared.error_handler_module import (
    error_handler,
    send_error_email
)

def test_error_handler_fail():
    """Testing that `error handler` raises a `TypeError` when argument/arguments are missing"""

    with pytest.raises(TypeError):
        error_handler(section='1.1')
    
def test_error_handler_success_with_data():
    """Testing if all arguments provided the then the function works as intended."""

    with patch("dpypelines.pipeline.shared.error_handler_module.logger") as mock_logger, \
         patch("dpypelines.pipeline.shared.error_handler_module.get_email_client", return_value=MagicMock()) as mock_email_client, \
         patch("dpypelines.pipeline.shared.error_handler_module.get_notifier", return_value=MagicMock()) as mock_notifier:

        error_handler(
            section = "1.2.1",
            error = "This is a Test Error",
            data={"TestKey":"Test value"},
            submitter_email="test@gmail.com",
            surpress_email=False,
            surpress_logs=False,
            surpress_notification=False 
        )

        #Test logger usage
        mock_logger.error.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error",
            data = {"TestKey":"Test value"}
        )

        #Test email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com',
            'ETL Pipeline error has occurred in Section: 1.2.1',
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error\n\n Additional Data: {'TestKey': 'Test value'}"
        )

        #Test notifier usage
        mock_notifier.return_value.failure.asser_called_once()

def test_error_handler_success_without_data():
    """Testing if not providing `data` which is optional, the function still works as intended."""

    with patch("dpypelines.pipeline.shared.error_handler_module.logger") as mock_logger, \
         patch("dpypelines.pipeline.shared.error_handler_module.get_email_client", return_value=MagicMock()) as mock_email_client, \
         patch("dpypelines.pipeline.shared.error_handler_module.get_notifier", return_value=MagicMock()) as mock_notifier:

        error_handler(
            section = "1.2.1",
            error = "This is a Test Error",
            data=None,
            submitter_email="test@gmail.com",
            surpress_email=False,
            surpress_logs=False,
            surpress_notification=False 
        )

        #Test logger usage
        mock_logger.error.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error"
        )

        #Test email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com',
            'ETL Pipeline error has occurred in Section: 1.2.1',
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error"
        )

        #Test notifier usage
        mock_notifier.return_value.failure.asser_called_once()

def test_send_error_email_success():
    """Testing if all arguments provided the then the function works as intended."""

    with patch("dpypelines.pipeline.shared.error_handler_module.get_email_client", return_value=MagicMock()) as mock_email_client:
        # Call the function with proper arguments
        send_error_email(
            section="1.2.1",
            error="Test email error",
            submitter_email="test@gmail.com",
            data={"extra": "details"}
        )

        # Validate email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com', 
            'ETL Pipeline error has occurred in Section: 1.2.1', 
            "An error has occurred in section: 1.2.1 \n\nTest email error\n\n Additional Data: {'extra': 'details'}")

def test_send_error_email_failure():
    """Testing that when an Exception is triggered the function behaves as expected."""

    with patch("dpypelines.pipeline.shared.error_handler_module.get_email_client", side_effect=Exception("Email client error")) as mock_email_client, \
         patch("dpypelines.pipeline.shared.error_handler_module.logger") as mock_logger:
        # Call the function with proper arguments
        send_error_email(
            section="2.2",
            error="Test email failure",
            submitter_email="test@gmail.com",
            data={"info": "test"}
        )

        # Validate that logger.error is called when email client fails
        mock_logger.error.assert_called_once_with(
            "Failed to send error email notification",
            Exception("Email client error")
        )