from typing import Optional

from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.messages.notification import BasePipelineNotifier
from dpypelines.pipeline.messages.utils import EmailClient

logger = DpLogger("data-ingress-pipeline")


def error_handler(
    error: Exception,
    data: Optional[dict],
    submitter_email: str,
    enable_logs: bool = True,
    enable_email: bool = True,
    enable_notification: bool = True,
    notifier: Optional[BasePipelineNotifier] = None,
    email_client: Optional[EmailClient] = None,
):
    """
    This function handles the errors.

    Arguments:
    error (str): The error message to log and process.
    data (dict, optional): Additional data to include in the logs.
    enable_logs (bool): If True, logging is enabled.
    enable_email (bool): If True, email notifications are enabled.
    enable_notification (bool): If True, system notifications are enabled.

    """

    # Log errors if `enable_logs` is True
    if enable_logs:
        if data:
            logger.info(f"Error in ETL pipeline: {error}", data=data)
        else:
            logger.info(f"Error in ETL pipeline: {error}")

    # Send email notification if `enable_email` is True
    if submitter_email and submitter_email != "" and enable_email and email_client:
        send_error_email(str(error), submitter_email, data, email_client)

    # Send Slack notifications if `enable_notification` is True
    if enable_notification:
        try:
            if notifier is None:
                raise Exception("notifier is None")
            notifier.failure()
        except Exception as notification_err:
            logger.error(
                "Failed to trigger system notifications", error=notification_err
            )

    raise error


def send_error_email(
    error: str,
    submitter_email: str,
    data: Optional[dict],
    email_client: EmailClient,
):
    try:
        email_subject = "ETL Pipeline error has occurred"
        email_message = f"An error has occurred:\n\n{error}"
        if data:
            email_message += f"\n\n Additional Data: {data}"
        email_client.send(submitter_email, email_subject, email_message)
    except Exception as email_err:
        logger.error("Failed to send error email notification", error=email_err)
