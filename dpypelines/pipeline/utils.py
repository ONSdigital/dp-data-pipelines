from pathlib import Path

from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.config import JobConfiguration
from dpypelines.pipeline.messages.email_templates import submission_processed_email
from dpypelines.pipeline.messages.notification import BasePipelineNotifier, NopNotifier, PipelineNotifier
from dpypelines.pipeline.messages.utils import (
    get_email_client,
    get_local_time,
    get_mimetype,
)

logger = DpLogger("data-ingress-pipelines")


def create_notifier(webhook: str, process_start_time=None) -> BasePipelineNotifier:
    """
    Create a variant of BasePipelineMessenger by passing in a webhook.
    Enables use of webhooks from the AWS secrets manager rather than env vars.
    """

    notifications_disabled = JobConfiguration().disable_notifications

    if notifications_disabled is True:
        return NopNotifier()

    return PipelineNotifier(webhook, process_start_time)


def get_notifier():
    # Create notifier from webhook env var
    try:
        process_start_time = get_local_time()
        notifier: BasePipelineNotifier = create_notifier(
            JobConfiguration().de_slack_webhook,
            process_start_time=process_start_time,
        )
        logger.info("Notifier created", data={"notifier": notifier})
        return notifier
    except Exception as err:
        logger.error("Error occurred when creating notifier", error=err)
        raise err


def setup_clients():
    """Set up clients for notification and email."""
    notifier = get_notifier()
    email_client = get_email_client()

    if not notifier or not email_client:
        err_msg = "Failed to set up notification or email client."
        raise RuntimeError(err_msg)

    logger.info(
        "Clients set up successfully",
        data={"notifier": notifier, "email_client": email_client},
    )
    return notifier, email_client


def upload_files(files_to_upload):
    """Upload files and send notifications."""
    upload_url = JobConfiguration().upload_service_url
    dataset_api_url = JobConfiguration().dataset_api_url
    if not upload_url or not dataset_api_url:
        err_msg = f"Required variables not set: UPLOAD_SERVICE_URL: {upload_url}, DATASET_API_URL: {dataset_api_url}."
        raise EnvironmentError(err_msg)

    upload_client = UploadServiceClient(upload_url)
    for required_file_path in files_to_upload:
        logger.info(
            "Uploading file to Upload Service API",
            data={"file_path": required_file_path, "upload_url": upload_url},
        )
        mimetype = get_mimetype(Path(required_file_path).suffix)
        if not mimetype:
            err_msg = f"Uploading file type {Path(required_file_path).suffix} not supported for file: {required_file_path}."
            raise NotImplementedError(err_msg)

        upload_client.upload_new(required_file_path, mimetype)

        logger.info(
            "File uploaded",
            data={"file_path": required_file_path, "upload_url": upload_url},
        )


def send_submission_confirmation(email_client, submitter_email):
    """Send submission confirmation email."""
    email_content = submission_processed_email()
    if not email_content:
        err_msg = "Submission email content is empty."
        raise ValueError(err_msg)

    email_client.send(submitter_email, email_content.subject, email_content.message)
    logger.info("Confirmation email sent", data={"submitter_email": submitter_email})
