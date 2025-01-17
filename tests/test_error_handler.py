import pytest

from unittest.mock import patch, MagicMock

from dpypelines.pipeline.shared.error_handler_module import error_handler

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
            "Error in section 1.2.1: This is a Test Error",
            data = {"TestKey":"Test value"}
        )

        #Test email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com',
            'ETL Pipeline error has occured in Section: 1.2.1',
            "An error has occured in section: 1.2.1: \n\nThis is a Test Error\n\n Additional Data: {'TestKey': 'Test value'}"
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
            "Error in section 1.2.1: This is a Test Error"
        )

        #Test email client usage
        mock_email_client.return_value.send.assert_called_once_with(
            'test@gmail.com',
            'ETL Pipeline error has occured in Section: 1.2.1',
            "An error has occured in section: 1.2.1: \n\nThis is a Test Error"
        )

        #Test notifier usage
        mock_notifier.return_value.failure.asser_called_once()