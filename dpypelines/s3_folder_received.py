from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.api.dataset_api import (
    create_dataset_api_service,
    validate_and_upload_metadata,
)
from dpypelines.pipeline.api.upload import create_upload_service_client, upload_files
from dpypelines.pipeline.config import get_job_config
from dpypelines.pipeline.messages.error_handler_module import error_handler
from dpypelines.pipeline.messages.notification import send_submission_confirmation
from dpypelines.pipeline.messages.setup import setup_clients
from dpypelines.pipeline.metadata.metadata_loader import MetadataLoader
from dpypelines.pipeline.process_zip_file import (
    copy_s3_processing_folder_to_destination_folder,
    delete_s3_processing_folder,
    process_zip_file,
)
from dpypelines.pipeline.validate_pipeline import (
    validate_manifest,
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
    notifier = None
    email_client = None
    try:
        job_config = get_job_config()

        # Set up clients.
        notifier, email_client = setup_clients(config=job_config)

        # Create local directory store from decompressed zip file and move files to S3 `processing` folder
        local_store, decompressed_file_dir, s3_processing_folder = process_zip_file(
            s3_object_name
        )
        dataset_api_service = create_dataset_api_service(job_config)
        upload_client = create_upload_service_client(job_config)

        # Validate configuration and files.
        manifest = validate_manifest(local_store)
        metadata = MetadataLoader(dataset_api_service, logger).load_metadata(
            manifest, local_store
        )

        # Upload metadata to Dataset API.
        if not job_config.skip_data_upload:
            metadata_submitted = validate_and_upload_metadata(
                metadata=metadata, dataset_api_service=dataset_api_service
            )

            if metadata_submitted:
                upload_files(
                    decompressed_file_dir,
                    metadata.distributions,
                    job_config,
                    upload_client,
                )

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
            notifier=notifier,
            email_client=email_client,
        )
        raise
