import os

from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.config import JobConfiguration
from dpypelines.pipeline.messages.error_handler_module import error_handler
from dpypelines.pipeline.process_zip_file import (
    copy_s3_processing_folder_to_destination_folder,
    delete_s3_processing_folder,
    process_zip_file,
)
from dpypelines.pipeline.utils import (
    send_submission_confirmation,
    setup_clients,
    upload_files,
    upload_metadata,
)
from dpypelines.pipeline.validate_pipeline import (
    retrieve_and_validate_manifest,
    validate_pipeline_files,
)

logger = DpLogger("data-ingress-pipeline")

# Global boolean variables for error handler, to allow easy setting
ENABLE_NOTIFICATION = True
ENABLE_LOGS = True
ENABLE_EMAIL = True


def start(s3_object_name: str, *args, **kwargs):
    """
    Handles the required behaviour when receiving a zip file indicated by an S3 object name.
    Allows extra arguments to be passed (if used as a secondary function in the pipeline config).

    Args:
        s3_object_name (str): The S3 object name of the zip file to be processed.
        *args: Optional extra positional arguments.
        **kwargs: Optional extra keyword arguments.
    """
    try:
        # Set up clients.
        notifier, email_client = setup_clients()

        # Create local directory store from decompressed zip file and move files to S3 `processing` folder
        local_store, decompressed_file_dir, s3_processing_folder = process_zip_file(
            s3_object_name
        )

        # Validate configuration and files.
        manifest = retrieve_and_validate_manifest(local_store)
        metadata = validate_pipeline_files(manifest, local_store)

        # Upload metadata to Dataset API.
        if not JobConfiguration().skip_data_upload:
            metadata_submitted = upload_metadata(
                metadata,
            )
            if metadata_submitted:
                # If metadata successfully submitted, upload data files to the Upload Service
                files_to_upload = [
                    decompressed_file_dir / distribution.file
                    for distribution in metadata.distributions
                ]
                upload_files(files_to_upload)

                # Copy all files in S3 "processing" folder to S3 "processed" folder
                copy_s3_processing_folder_to_destination_folder(
                    s3_object_name,
                    decompressed_file_dir,
                    s3_processing_folder,
                    "processed",
                )

                send_submission_confirmation(
                    email_client, manifest.submission_contacts[0].email
                )

                notifier.success()
                logger.info("ETL process completed successfully")

                # Delete files from S3 "processing" folder
                delete_s3_processing_folder(
                    s3_object_name, decompressed_file_dir, s3_processing_folder
                )
                return True
            else:
                # Copy all files in S3 "processing" folder to S3 "dataset-type-not-static" folder
                copy_s3_processing_folder_to_destination_folder(
                    s3_object_name,
                    decompressed_file_dir,
                    s3_processing_folder,
                    "dataset-type-not-static",
                )
                # Delete files from S3 "processing" folder
                delete_s3_processing_folder(
                    s3_object_name, decompressed_file_dir, s3_processing_folder
                )
                return False

    except Exception as err:
        logger.error("ETL process failed", err)
        error_handler(
            section="ETL",
            error=err,
            data={"s3_object_name": s3_object_name},
            submitter_email=(
                manifest.submission_contacts[0].email if "manifest" in locals() else ""
            ),
            enable_email=ENABLE_EMAIL,
            enable_logs=ENABLE_LOGS,
            enable_notification=ENABLE_NOTIFICATION,
        )
        notifier.failure()
        raise
