import os
import re
from pathlib import Path

from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_templates import (
    submission_processed_email,
    successful_file_upload_email,
)
from dpypelines.pipeline.shared.error_handler_module import error_handler
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_mimetype,
)
from dpypelines.pipeline.utils import get_notifier
from dpypelines.pipeline.validate_pipeline import validate_pipeline_files

logger = DpLogger("data-ingress-pipelines")


def dataset_ingress_v1(files_dir: str, pipeline_config: dict):
    """
    Version 1 of the dataset ingress pipeline.

    Args:
        files_dir (str): Path to the directory where the input files for this pipeline are located.
        pipeline_config (dict): Dictionary of configuration details required to run the pipeline (determined by dataset id)

    Raises:
        Exception: If any unexpected error occurs.
    """
    files_dir = Path(files_dir)

    # Validate the pipeline
    try:
        validation_results = validate_pipeline_files(files_dir, pipeline_config)
        logger.info("Pipeline validation completed successfully", data={"files_dir": str(files_dir)})
    except Exception as e:
        logger.error("Pipeline validation failed", error=e, data={"files_dir": str(files_dir)})
        raise

    # Create notifier from webhook env var
    de_notifier = get_notifier()

    # Create boolean variables for error handler, to allow easy setting
    enable_notification = True
    enable_logs = True
    enable_email = True

    # Create local data store from files directory
    try:
        local_store = LocalDirectoryStore(files_dir)
        files_in_directory = local_store.get_file_names()
        logger.info(
            "Local data store created",
            data={
                "local_store": local_store,
                "local_store_dir": files_dir,
                "files_in_directory": files_in_directory,
            },
        )
    except Exception:
        error_handler(
            section="1.1",
            error="Failed to create local data store from files directory",
            data={"files_directory": files_dir},
            submitter_email="",
            enable_email=enable_email,
            enable_logs=enable_logs,
            enable_notification=enable_notification,
        )

    # Retrieve submitter email from validation results and create email client from env var
    try:
        submitter_email = validation_results["manifest"]["fileAuthorEmail"]
        email_client = get_email_client()
        logger.info(
            "Submitter email received, email client created",
            data={"email_client": email_client},
        )
    except Exception:
        error_handler(
            section="1.1",
            error="Failed to create email client",
            data=None,
            submitter_email="No submitter email acquired",
            enable_email=enable_email,
            enable_logs=enable_logs,
            enable_notification=enable_notification,
        )

    # Allow DE's to skip uploading to S3 while developing code locally.
    # Retrieve SKIP_DATA_UPLOAD value from environment variable
    skip_data_upload = os.environ.get("SKIP_DATA_UPLOAD", "False")
    skip_data_upload = str_to_bool(skip_data_upload)

    # Retrieve Upload Service URL from environment variable
    if not skip_data_upload:
        try:
            upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
            assert (
                upload_url is not None
            ), "UPLOAD_SERVICE_URL environment variable not set"
        except Exception:
            error_handler(
                section="1.1",
                error="Failed to retrieve Upload Service URL",
                data=None,
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    # Retrieve Upload Service URL from environment variable
    if not skip_data_upload:
        try:
            upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
            assert (
                upload_url is not None
            ), "UPLOAD_SERVICE_URL environment variable not set"
        except Exception as err:
            logger.error("Failed to retrieve Upload Service URL", err)
            de_notifier.failure()
            raise err

        try:
            # Create UploadClient from upload_url
            upload_client = UploadServiceClient(upload_url)
        except Exception:
            error_handler(
                section="1.1",
                error="Failed to create UploadClient",
                data={"upload_url": upload_url},
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

        try:
            for required_file_path in validation_results["config_files"]:
                mimetype = get_mimetype(Path(required_file_path).suffix)
                if mimetype:
                    upload_client.upload_new(required_file_path, mimetype)
                else:
                    raise NotImplementedError(
                        f"Uploading file type {Path(required_file_path).suffix} not currently supported."
                    )
                logger.info(
                    "File uploaded",
                    data={
                        "file_path": required_file_path,
                        "upload_url": upload_url,
                    },
                )
                email_content = successful_file_upload_email(
                    Path(required_file_path).name
                )
                email_client.send(
                    submitter_email, email_content.subject, email_content.message
                )
        except Exception:
            error_handler(
                section="1.1",
                error="Failed to upload file",
                data={"file_path": required_file_path},
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

    email_content = submission_processed_email()
    email_client.send(submitter_email, email_content.subject, email_content.message)
    de_notifier.success()
    return True
