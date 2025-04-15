import os
from pathlib import Path
from typing import List

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.dataset_api import (
    check_dataset_type_is_static,
    get_post_request_values_from_metadata,
)
from dpypelines.pipeline.messages.email_templates import submission_processed_email
from dpypelines.pipeline.messages.notification import (
    PipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.messages.utils import (
    get_email_client,
    get_local_time,
    get_mimetype,
)
from dpypelines.pipeline.models import Metadata

logger = DpLogger("data-ingress-pipelines")


def get_notifier():
    # Create notifier from webhook env var
    try:
        process_start_time = get_local_time()
        notifier: PipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK",
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


def upload_metadata(metadata: Metadata) -> bool:
    """
    Upload metadata to the Dataset API.
    """
    dataset_api_url = os.environ.get("DATASET_API_URL")
    if not dataset_api_url:
        msg = "Required environment variable not set: DATASET_API_URL"
        raise EnvironmentError(msg)

    # Generate POST request body from metadata
    dataset_path, edition_path, request_body = get_post_request_values_from_metadata(
        metadata
    )
    dataset_api_client = DatasetAPIClient(dataset_api_url, dataset_path, edition_path)

    # Upload metadata only if the dataset type is "static"
    if check_dataset_type_is_static(dataset_api_client):
        # Verify that the relevant Dataset API endpoint exists
        get_versions_path_response = dataset_api_client.get_path()

        # If the endpoint exists, send POST request
        if get_versions_path_response.status_code != 200:
            get_versions_path_response.raise_for_status()
        else:
            logger.info(
                "Dataset API endpoint exists",
                data={"dataset_api_endpoint": dataset_api_client.full_url},
            )

            post_json_response = dataset_api_client.post_json(request_body)

            if post_json_response.status_code == 201:
                logger.info(
                    "Metadata submitted to Dataset API endpoint",
                    data={"dataset_api_endpoint": dataset_api_client.full_url},
                )
                return True
    return False


def upload_files(files_to_upload: List[Path]):
    """
    Upload data files to the Upload Service.
    """
    upload_url = os.environ.get("UPLOAD_SERVICE_URL")
    if not upload_url:
        err_msg = "Required environment variable not set: UPLOAD_SERVICE_URL."
        raise EnvironmentError(err_msg)

    upload_client = UploadServiceClient(upload_url)
    for required_file_path in files_to_upload:
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
