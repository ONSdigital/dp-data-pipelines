import os
from pathlib import Path

from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import s3_folder_recieved
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.shared.email_templates import (
    submission_processed_email,
    successful_file_upload_email,
)
from dpypelines.pipeline.shared.error_handler_module import error_handler
from dpypelines.pipeline.shared.utils import get_email_client, get_mimetype
from dpypelines.pipeline.utils import get_notifier, get_source_id
from dpypelines.pipeline.validate_pipeline import validate_pipeline_files

logger = DpLogger("data-ingress-pipeline")

# Global boolean variables for error handler, to allow easy setting
ENABLE_NOTIFICATION = True
ENABLE_LOGS = True
ENABLE_EMAIL = True


def start(s3_object_name: str):
    """
    Handles the required behaviour when receiving a `.tar` file indicated by an s3 object name.

    Args:
        s3_object_name (str): The S3 object name of the tar file to be processed.
    """
    try:
        # Step 1: Set up clients.
        notifier, email_client = setup_clients()

        # Step 2: Validate configuration and files.
        local_store = decompress_tar_file(s3_object_name)

        manifest_dict, pipeline_config, files_dir = retrieve_config_and_files_dir(
            local_store
        )
        validation_results = validate_pipeline(files_dir, pipeline_config)

        # Step 3: Send Data & Metadata.
        if not str_to_bool(os.environ.get("SKIP_DATA_UPLOAD", "False")):
            upload_files(
                validation_results, email_client, manifest_dict["fileAuthorEmail"]
            )
        send_submission_confirmation(email_client, manifest_dict["fileAuthorEmail"])

        notifier.success()
        logger.info("ETL process completed successfully")
        return True

    except Exception as err:
        logger.error("ETL process failed", err)
        error_handler(
            section="ETL",
            error=str(err),
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


def setup_clients():
    """Set up clients for notification and email."""
    notifier = get_notifier()
    email_client = get_email_client()

    if not notifier or not email_client:
        err_msg = "Failed to set up notification or email client."
        logger.error(err_msg)
        raise RuntimeError(err_msg)

    logger.info(
        "Clients set up successfully",
        data={"notifier": notifier, "email_client": email_client},
    )
    return notifier, email_client


def decompress_tar_file(s3_object_name):
    """Decompress the tar file to the local directory."""
    s3_folder_recieved(str(s3_object_name), "input")
    local_store = LocalDirectoryStore("input")
    files = local_store.get_file_names()

    if not files:
        err_msg = f"Decompressed directory 'input' is empty for s3_object: {s3_object_name}. Available files: {files}"
        logger.error(err_msg, data={"local_store": files})
        raise FileNotFoundError(err_msg)

    logger.info(
        "S3 `.tar` object received and decompressed to ./input",
        data={"s3_object_name": s3_object_name},
    )
    return local_store


def retrieve_config_and_files_dir(local_store):
    """Retrieve configuration and files from the local directory."""
    manifest_dict = retrieve_manifest(local_store)
    source_id = get_source_id_from_manifest(manifest_dict)
    pipeline_config = get_pipeline_config_for_source(source_id)
    files_dir = local_store.get_current_source_pathlike()

    if not manifest_dict or not source_id or not pipeline_config or not files_dir:
        err_msg = "Failed to retrieve configuration and files from the local directory."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    logger.info(
        "Configuration and files retrieved successfully",
        data={
            "manifest_dict": manifest_dict,
            "source_id": source_id,
            "pipeline_config": pipeline_config,
            "files_dir": str(files_dir),
        },
    )

    return manifest_dict, pipeline_config, files_dir


def retrieve_manifest(local_store):
    """Retrieve the manifest.json file from the local directory."""
    manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")

    if not manifest_dict:
        err_msg = "manifest.json not found in the local store."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)
    return manifest_dict


def get_source_id_from_manifest(manifest_dict):
    """Extract the source_id from the manifest."""
    source_id = get_source_id(manifest_dict)
    if not source_id:
        err_msg = f"source_id not found within manifest: {manifest_dict}."
        logger.error(err_msg)
        raise KeyError(err_msg)
    return source_id


def get_pipeline_config_for_source(source_id):
    """Retrieve pipeline configuration using source_id."""
    pipeline_config = get_pipeline_config(source_id)
    if not pipeline_config:
        err_msg = f"Pipeline configuration not found for source_id: {source_id}."
        logger.error(err_msg)
        raise ValueError(err_msg)
    return pipeline_config


def validate_pipeline(files_dir, pipeline_config):
    """Validate the pipeline files against the configuration."""
    validation_results = validate_pipeline_files(files_dir, pipeline_config)

    if not validation_results.get("manifest"):
        err_msg = f"Manifest validation failed for files in {files_dir} using config: {pipeline_config}."
        logger.error(
            err_msg,
            data={
                "files_dir": str(files_dir),
                "validation_results": validation_results,
            },
        )
        raise ValueError(err_msg)

    logger.info(
        "Pipeline validation completed successfully", data={"files_dir": str(files_dir)}
    )
    return validation_results


def upload_files(validation_results, email_client, submitter_email):
    """Upload files and send notifications."""
    upload_url = os.environ.get("UPLOAD_SERVICE_URL")
    dataset_api_url = os.environ.get("DATASET_API_URL")
    if not upload_url or not dataset_api_url:
        err_msg = (
            f"Required environment variable(s) not set: "
            f"UPLOAD_SERVICE_URL: {upload_url}, DATASET_API_URL: {dataset_api_url}."
        )
        logger.error(err_msg)
        raise EnvironmentError(err_msg)

    upload_client = UploadServiceClient(upload_url)
    for file_path in validation_results["config_files"]:
        mimetype = get_mimetype(Path(file_path).suffix)
        if not mimetype:
            err_msg = f"Uploading file type {Path(file_path).suffix} not supported for file: {file_path}."
            logger.error(err_msg)
            raise NotImplementedError(err_msg)

        upload_client.upload_new(file_path, mimetype)
        logger.info(
            "File uploaded", data={"file_path": file_path, "upload_url": upload_url}
        )
        email_content = successful_file_upload_email(Path(file_path).name)
        email_client.send(submitter_email, email_content.subject, email_content.message)
        logger.info(
            "Upload notification email sent",
            data={"submitter_email": submitter_email, "file": file_path},
        )


def send_submission_confirmation(email_client, submitter_email):
    """Send submission confirmation email."""
    email_content = submission_processed_email()
    if not email_content:
        err_msg = "Submission email content is empty."
        logger.error(err_msg)
        raise ValueError(err_msg)
    email_client.send(submitter_email, email_content.subject, email_content.message)
    logger.info("Confirmation email sent", data={"submitter_email": submitter_email})
