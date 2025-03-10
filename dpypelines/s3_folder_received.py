import os

from dpytools.logging.logger import DpLogger
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.error_handler_module import error_handler
from dpypelines.pipeline.utils import (
    decompress_file,
    send_submission_confirmation,
    setup_clients,
    upload_files,
    upload_metadata,
    validate_pipeline,
)
from dpypelines.pipeline.validate_pipeline import retrieve_config_and_files

logger = DpLogger("data-ingress-pipeline")

# Global boolean variables for error handler, to allow easy setting
ENABLE_NOTIFICATION = True
ENABLE_LOGS = True
ENABLE_EMAIL = True


def start(s3_object_name: str, *args, **kwargs):
    """
    Handles the required behaviour when receiving a `.tar` file indicated by an s3 object name.
    Allows extra arguments to be passed (if used as a secondary function in the pipeline config).

    Args:
        s3_object_name (str): The S3 object name of the tar file to be processed.
        *args: Optional extra positional arguments.
        **kwargs: Optional extra keyword arguments.
    """
    try:
        # Step 1: Set up clients.
        notifier, email_client = setup_clients()

        # Step 2: Validate configuration and files.
        local_store = decompress_file(s3_object_name, directory="input")

        manifest_dict, pipeline_config, files_dir = retrieve_config_and_files(
            local_store
        )
        validation_results = validate_pipeline(files_dir, pipeline_config)

        # Step 3: Upload Files & Metadata.
        if not str_to_bool(os.environ.get("SKIP_DATA_UPLOAD", "False")):
            upload_files(
                validation_results, email_client, manifest_dict["fileAuthorEmail"]
            )
            upload_metadata(local_store, email_client, manifest_dict["fileAuthorEmail"])
        send_submission_confirmation(email_client, manifest_dict["fileAuthorEmail"])

        notifier.success()
        logger.info("ETL process completed successfully")
        return True

    except Exception as err:
        logger.error("ETL process failed", err)
        error_handler(
            section="ETL",
            error=err,
            data={"s3_object_name": s3_object_name},
            submitter_email=(
                manifest_dict.get("fileAuthorEmail", "")
                if "manifest_dict" in locals()
                else ""
            ),
            enable_email=ENABLE_EMAIL,
            enable_logs=ENABLE_LOGS,
            enable_notification=ENABLE_NOTIFICATION,
        )
        notifier.failure()
        raise
