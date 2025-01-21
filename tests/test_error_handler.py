from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.shared.error_handler_module import (
    error_handler,
    send_error_email,
)

class TestErrorHandler:
    #Decorator to execute fixture for every test in the class
    @pytest.fixture(autouse=True)

    #Setting up the Mock patches
    def setup_mock(self):
        self.mock_logger_patch = patch("dpypelines.pipeline.shared.error_handler_module.logger")
        self.mock_email_client_patch = patch("dpypelines.pipeline.shared.error_handler_module.get_email_client", return_value=MagicMock())
        self.mock_notifier_patch = patch("dpypelines.pipeline.shared.error_handler_module.get_notifier", return_value=MagicMock())

        self.mock_logger = self.mock_logger_patch.start()
        self.mock_email_client = self.mock_email_client_patch.start()
        self.mock_notifier = self.mock_notifier_patch.start()

        yield

        patch.stopall()

    def test_error_handler_fail(self):
        """Testing that `error handler` raises a `TypeError` when argument/arguments are missing"""

        with pytest.raises(TypeError):
            error_handler(section='1.1')
        
    def test_error_handler_success_with_data(self):
        """Testing if all arguments provided the then the function works as intended."""

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
        self.mock_logger.error.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error",
            data = {"TestKey":"Test value"}
        )

        #Test email client usage
        self.mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com',
            'ETL Pipeline error has occurred in Section: 1.2.1',
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error\n\n Additional Data: {'TestKey': 'Test value'}"
        )

        #Test notifier usage
        self.mock_notifier.return_value.failure.asser_called_once()

    def test_error_handler_success_without_data(self):
        """Testing if not providing `data` which is optional, the function still works as intended."""

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
        self.mock_logger.error.assert_called_once_with(
            "Error in section: 1.2.1 This is a Test Error"
        )

        #Test email client usage
        self.mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com',
            'ETL Pipeline error has occurred in Section: 1.2.1',
            "An error has occurred in section: 1.2.1 \n\nThis is a Test Error"
        )

        #Test notifier usage
        self.mock_notifier.return_value.failure.asser_called_once()

    def test_send_error_email_success(self):
        """Testing if all arguments provided the then the function works as intended."""

        # Call the function with proper arguments
        send_error_email(
            section="1.2.1",
            error="Test email error",
            submitter_email="test@gmail.com",
            data={"extra": "details"}
        )

        # Validate email client usage
        self.mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com', 
            'ETL Pipeline error has occurred in Section: 1.2.1', 
            "An error has occurred in section: 1.2.1 \n\nTest email error\n\n Additional Data: {'extra': 'details'}")

    def test_send_error_email_failure(self):
        """Testing that when an Exception is triggered the function behaves as expected."""

        # Simulate failure in the send method (also git was complaining that the variable wasn't used)
        self.mock_email_client.return_value.send.side_effect = Exception("Send failure")

        # Call the function with proper arguments
        send_error_email(
            section="2.2",
            error="Test email failure",
            submitter_email="test@gmail.com",
            data={"info": "test"}
        )

        # Validate that logger.error was called
        self.mock_logger.error.assert_called_once()

        # Extract the actual call arguments
        log_call_args = self.mock_logger.error.call_args

        # Validate the first argument (message)
        assert log_call_args[0][0] == "Failed to send error email notification"

        # Validate the second argument (exception)
        assert isinstance(log_call_args[0][1], Exception)
        assert str(log_call_args[0][1]) == "Send failure"