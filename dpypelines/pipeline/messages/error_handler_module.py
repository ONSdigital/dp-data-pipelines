from typing import Optional

from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.messages.utils import get_email_client
from dpypelines.pipeline.utils import get_notifier

logger = DpLogger("data-ingress-pipelines")


def error_handler(
    section: str,
    error: Exception,
    data: Optional[dict],
    submitter_email: str,
    enable_logs: bool = True,
    enable_email: bool = True,
    enable_notification: bool = True,
):
    """
    This function handles the errors.

    Arguments:
    section (str): The section of the ETL pipeline where the error occurred.
    error (str): The error message to log and process.
    data (dict, optional): Additional data to include in the logs.
    enable_logs (bool): If True, logging is surpressed.
    enable_email (bool): If True, email notifications are surpressed.
    enable_notification (bool): If True, system notifications are surpressed.

    """

    # Log errors if the `surpress_logs is set to false
    if enable_logs:
        if data:
            logger.info(f"Error in section: {section} {error}", data=data)
        else:
            logger.info(f"Error in section: {section} {error}")

    # Send email notification if `surpress_email` is set to false
    if submitter_email and enable_email:
        send_error_email(section, str(error), submitter_email, data)

    # Send system notifiations if `surpress_notification` is set to false
    if enable_notification:
        try:
            notifier = get_notifier()
            notifier.failure()
        except Exception as notification_err:
            logger.error("Failed to trigger system notifications", notification_err)

    raise Exception(error)


def send_error_email(
    section: str, error: str, submitter_email: str, data: Optional[dict]
):
    try:
        email_client = get_email_client()
        email_subject = f"ETL Pipeline error has occurred in Section: {section}"
        email_message = f"An error has occurred in section: {section} \n\n{error}"
        if data:
            email_message += f"\n\n Additional Data: {data}"
        email_client.send(submitter_email, email_subject, email_message)
    except Exception as email_err:
        logger.error("Failed to send error email notification", email_err)
