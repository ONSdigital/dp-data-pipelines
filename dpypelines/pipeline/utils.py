import os
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Tuple

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import _get_s3_client, upload_local_file_to_s3
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.errors import (
    DatasetAPIRequestCreationException,
    DistributionsException,
    ValidationException,
)
from dpypelines.pipeline.job_configuration import secrets_job_config
from dpypelines.pipeline.messages.email_templates import (
    submission_processed_email,
    successful_file_upload_email,
    successful_metadata_submission,
)
from dpypelines.pipeline.messages.notification import NopNotifier, PipelineNotifier
from dpypelines.pipeline.messages.utils import (
    get_email_client,
    get_local_time,
    get_mimetype,
    str_to_bool,
)
from dpypelines.pipeline.validate_pipeline import validate_pipeline_files

logger = DpLogger("data-ingress-pipelines")


def create_notifier(webhook: str, process_start_time=None):
    """
    Create a variant of BasePipelineMessenger by passing in a webhook.
    Enables use of webhooks from the AWS secrets manager rather than env vars.
    """

    notifications_disabled = os.environ.get("DISABLE_NOTIFICATIONS", None)
    notifications_disabled = (
        False if notifications_disabled is None else str_to_bool(notifications_disabled)
    )

    if notifications_disabled is True:
        return NopNotifier()

    return PipelineNotifier(webhook, process_start_time)


def get_notifier():
    # Create notifier from webhook env var
    try:
        process_start_time = get_local_time()
        notifier: PipelineNotifier = create_notifier(
            str(secrets_job_config.de_slack_webhook),
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


def download_zip_file(s3_object_name: str) -> str:
    """
    Downloads a zip file from S3 into a local directory and returns the local file path.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)

    # Split s3_object_name on final "/" in case of nested directory structure (e.g. <dir1>/dir2>/input.zip)
    input_dir, input_zip_name = object_key.rsplit("/", maxsplit=1)
    # Create a local directory to store downloaded zip file
    Path(input_dir).mkdir(parents=True, exist_ok=True)

    # Download S3 object to local directory
    client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))
    with open(object_key, "wb") as f:
        client.download_fileobj(Bucket=bucket_name, Key=object_key, Fileobj=f)
    logger.info(
        "Downloaded zip file to local folder",
        data={"local_input_folder": input_dir, "local_file_name": input_zip_name},
    )
    return object_key


def decompress_zip_file(local_zip_path: str) -> Path:
    """
    Decompress the given zip file into a local folder named after the input zip file (without extension).
    """
    # Create destination directory to store decompressed files
    _, file_name = local_zip_path.rsplit("/", maxsplit=1)
    destination_dir = Path(file_name.split(".")[0])
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Extract zip file to destination directory
    with zipfile.ZipFile(local_zip_path, "r") as f:
        f.extractall(destination_dir)

    # Handle different zip methods (zip files behave differently depending on operating system etc)
    if any([f.is_dir() for f in destination_dir.rglob("*")]):
        decompressed_file_dir = destination_dir / destination_dir
    else:
        decompressed_file_dir = destination_dir

    logger.info(
        "Decompressed zip file",
        data={
            "local_zip_path": str(local_zip_path),
            "decompressed_file_dir": str(decompressed_file_dir),
        },
    )
    return decompressed_file_dir


def upload_to_s3_processing_folder(
    s3_object_name: str,
    local_object_key: str,
    decompressed_file_dir: Path,
) -> str:
    """
    Generate "processing" S3 folder name with timestamp and upload decompressed files/copy input zip file to the generated S3 folder.
    """
    bucket_name, s3_object_key = s3_object_name.split("/", maxsplit=1)
    s3_processing_folder = f"processing/{datetime.now().strftime('%y-%m-%dT%H-%M')}-{decompressed_file_dir.parts[-1]}"
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Upload unzipped files to S3 "processing" folder
    for file_path in decompressed_file_dir.rglob("*"):
        # Delete hidden artefacts e.g. .DS_Store
        if file_path.stem.startswith("."):
            os.remove(file_path)
        else:
            s3_processing_object_name = (
                f"{bucket_name}/{s3_processing_folder}/{file_path.name}"
            )
            upload_local_file_to_s3(
                f"{decompressed_file_dir}/{file_path.name}",
                s3_processing_object_name,
                os.environ.get("AWS_PROFILE"),
            )
    logger.info("Decompressed files uploaded to S3 'processing' folder")

    # Copy original zip file from S3 input location to S3 "processing" folder
    s3_client.copy_object(
        Bucket=bucket_name,
        Key=f"{s3_processing_folder}/{local_object_key}",
        CopySource={"Bucket": bucket_name, "Key": s3_object_key},
    )

    # Delete original zip file from S3 input location
    s3_client.delete_object(Bucket=bucket_name, Key=s3_object_key)
    logger.info(
        "Input zip file copied to S3 'processing' folder and deleted from input folder"
    )
    return s3_processing_folder


def copy_s3_processing_folder_to_processed_folder(
    s3_object_name: str,
    decompressed_file_dir: Path,
    s3_processing_folder: str,
) -> str:
    """
    Copy all files in S3 "processing" folder to S3 "processed" folder.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)
    s3_processed_folder = f"processed/{datetime.now().strftime('%y-%m-%dT%H-%M')}-{decompressed_file_dir.parts[-1]}"
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Copy unzipped files from S3 "processing" folder to "processed" folder
    for file_path in decompressed_file_dir.rglob("*"):
        s3_client.copy_object(
            Bucket=bucket_name,
            Key=f"{s3_processed_folder}/{file_path.name}",
            CopySource={
                "Bucket": bucket_name,
                "Key": f"{s3_processing_folder}/{file_path.name}",
            },
        )

    # Copy original zip file from S3 "processing" folder to "processed" folder
    s3_client.copy_object(
        Bucket=bucket_name,
        Key=f"{s3_processed_folder}/{object_key}",
        CopySource={
            "Bucket": bucket_name,
            "Key": f"{s3_processing_folder}/{object_key}",
        },
    )
    logger.info(
        "Decompressed files and original zip submission copied to S3 'processed' folder",
        data={"s3_processed_folder": s3_processed_folder},
    )

    return s3_processed_folder


def delete_s3_processing_folder(
    s3_object_name: str, decompressed_file_dir: Path, s3_processing_folder: str
) -> None:
    """
    Delete all files from S3 "processing" folder to indicate successful submission.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Delete unzipped files from S3 "processing" folder
    for file_path in decompressed_file_dir.rglob("*"):
        s3_client.delete_object(
            Bucket=bucket_name, Key=f"{s3_processing_folder}/{file_path.name}"
        )

    # Delete zip file from S3 "processing" folder
    s3_client.delete_object(
        Bucket=bucket_name, Key=f"{s3_processing_folder}/{object_key}"
    )
    logger.info(
        "Decompressed files and original zip submission deleted from S3 'processing' folder"
    )


def process_zip_file(s3_object_name: str) -> Tuple[LocalDirectoryStore, Path, str]:
    """
    Download a zip file from S3, decompress it, upload decompressed files to S3 'processing' folder and create a LocalDirectoryStore from the decompressed files.

    Returns:
        LocalDirectoryStore: A local store representing the processed folder.
    """
    # Download the zip file.
    local_object_key = download_zip_file(s3_object_name)

    # Decompress the zip file locally.
    decompressed_file_dir = decompress_zip_file(local_object_key)

    # Generate "processing" S3 folder name with timestamp and upload decompressed files
    s3_processing_folder = upload_to_s3_processing_folder(
        s3_object_name, local_object_key, decompressed_file_dir
    )

    # Validate that the decompressed folder contains files and create LocalDirectoryStore.
    local_store = LocalDirectoryStore(decompressed_file_dir)
    files = local_store.get_file_names()
    if not files:
        err_msg = f"Decompressed directory {decompressed_file_dir} is empty for s3_object_name {s3_object_name}. Available files: {files}"
        raise FileNotFoundError(err_msg)

    logger.info(
        "S3 zip object processed successfully",
        data={
            "s3_object_name": s3_object_name,
            "decompressed_file_dir": str(decompressed_file_dir),
        },
    )
    return local_store, decompressed_file_dir, s3_processing_folder


def validate_pipeline(files_dir: Path, pipeline_config: dict) -> dict:
    """Validate the pipeline files against the configuration."""
    validation_results = validate_pipeline_files(files_dir, pipeline_config)

    try:
        validation_results.get("manifest")
    except Exception:
        err_msg = f"Manifest validation failed for files in {files_dir} using config: {pipeline_config}."
        raise ValidationException(
            err_msg, files_dir=files_dir, validation_results=validation_results
        )

    logger.info(
        "Pipeline validation completed successfully", data={"files_dir": str(files_dir)}
    )
    return validation_results


def get_post_request_values_from_metadata(metadata: dict):
    """
    Generate the required path values and request body to submit to the Dataset API. This will be submitted as a POST request to the endpoint `/datasets/{dataset_path}/editions/{edition_path}/versions`
    """
    try:
        dataset_path = metadata.get("dcterms:identifier", None)
        editions = metadata.get("TBC:edition", None)
        if editions is not None:
            edition: dict = editions[0]
        else:
            edition = {"dcterms:identifier": None}
        edition_path = edition.get("dcterms:identifier", None)
        dcat_distributions = edition.get("dcat:distribution", None)
        if dcat_distributions is not None:
            distributions = get_distribution_details_for_request(dcat_distributions)
        else:
            distributions = None

        request_body = {
            # Required properties (from swagger.yaml)
            "distributions": distributions,
            "release_date": "The release date of this version of the dataset",
            # Tier 0 metadata standards - required with output
            "title": metadata.get("dcterms:title", None),
            "description": metadata.get("dcterms:description", None),
            "next_release": metadata.get("TBC:nextRelease", None),
            # Tier 0 metadata standards - added during publishing
            "type": "static",
            "state": "associated",
            "themes": metadata.get("dcat:theme", None),
            # Additional properties (from swagger.yaml)
            "alerts": edition.get("TBC:alerts", None),
            "quality_designation": edition.get("TBC:quality_designation", None),
            "usage_notes": edition.get("TBC:usage_notes", None),
            # TODO `links:spatial` and `links:job` fields to be removed from data model - hardcode for now to allow request to succeed
            "links": {
                "spatial": {"href": "string"},
                "job": {"href": "string", "id": "string"},
            },
            # Not included here ($ref: '#/definitions/Version')
            # collection_id (auto generated?)
            # dimensions $ref: '#/definitions/Dimension'
            # edition (readOnly - auto generated?)
            # dataset_id (auto generated?)
            # is_based_on (census only)
            # last_updated (readOnly - auto generated?)
            # latest_changes $ref: '#/definitions/LatestChange'
            # links (auto generated?)
            # lowest_geography (census only)
            # temporal $ref: '#/definitions/Temporal'
            # version (readOnly - auto generated?)
        }
        return dataset_path, edition_path, request_body
    except Exception as err:
        raise DatasetAPIRequestCreationException(
            "Error getting POST request values from metadata", metadata=metadata
        ) from err


def get_distribution_details_for_request(dcat_distributions: list) -> list:
    """
    Get the information to populate the `distributions` property in the POST request to the Dataset API.
    """
    try:
        distributions = [
            {
                "title": distribution["dcterms:title"],
                "download_url": "The URL to the generated file",
                # TODO Calculate byte_size during processing
                "byte_size": "The size of the file in bytes",
                "format": distribution["TBC:distributionFormat"],
                "media_type": distribution["dcat:mediaType"],
            }
            for distribution in dcat_distributions
        ]
        logger.info(
            "Distributions information retrieved",
            data={"distributions": distributions},
        )
        return distributions
    except Exception as err:
        raise DistributionsException(
            "Error getting details of distributions for Dataset API request",
            distributions=dcat_distributions,
        ) from err


def upload_metadata(local_store, email_client, submitter_email):
    """Upload metadata and send notifications."""
    dataset_api_url = secrets_job_config.dataset_api_url
    if not dataset_api_url:
        err_msg = (
            f"Required environment variable(s) not set: "
            f"DATASET_API_URL: {dataset_api_url}."
        )
        raise EnvironmentError(err_msg)

    metadata = local_store.get_lone_matching_json_as_dict("^metadata.json$")
    if not metadata:
        err_msg = "metadata.json not found in the local store."
        raise FileNotFoundError(err_msg)

    dataset_path, edition_path, request_body = get_post_request_values_from_metadata(
        metadata
    )
    dataset_api_client = DatasetAPIClient(dataset_api_url, dataset_path, edition_path)
    dataset_api_get_path_response = dataset_api_client.get_path()
    logger.info(
        "Dataset API endpoint exists",
        data={"dataset_api_endpoint": dataset_api_client.full_url},
    )

    if dataset_api_get_path_response.status_code == 200:
        dataset_api_client.post_json(request_body)
        logger.info(
            "Metadata submitted to Dataset API endpoint",
            data={"dataset_api_endpoint": dataset_api_client.full_url},
        )
        email_content = successful_metadata_submission(dataset_path)
        email_client.send(submitter_email, email_content.subject, email_content.message)
    else:
        dataset_api_get_path_response.raise_for_status()


def upload_files(validation_results, email_client, submitter_email):
    """Upload files and send notifications."""
    upload_url = secrets_job_config.upload_service_url
    dataset_api_url = secrets_job_config.dataset_api_url
    if not upload_url or not dataset_api_url:
        err_msg = (
            f"Required environment variable(s) not set: "
            f"UPLOAD_SERVICE_URL: {upload_url}, DATASET_API_URL: {dataset_api_url}."
        )
        raise EnvironmentError(err_msg)

    upload_client = UploadServiceClient(upload_url)
    for required_file_path in validation_results["config_files"]:
        mimetype = get_mimetype(Path(required_file_path).suffix)
        if not mimetype:
            err_msg = f"Uploading file type {Path(required_file_path).suffix} not supported for file: {required_file_path}."
            raise NotImplementedError(err_msg)

        upload_client.upload_new(required_file_path, mimetype)
        logger.info(
            "File uploaded",
            data={"file_path": required_file_path, "upload_url": upload_url},
        )
        email_content = successful_file_upload_email(Path(required_file_path).name)
        email_client.send(submitter_email, email_content.subject, email_content.message)
        logger.info(
            "Upload notification email sent",
            data={"submitter_email": submitter_email, "file": required_file_path},
        )


def send_submission_confirmation(email_client, submitter_email):
    """Send submission confirmation email."""
    email_content = submission_processed_email()
    if not email_content:
        err_msg = "Submission email content is empty."
        raise ValueError(err_msg)

    email_client.send(submitter_email, email_content.subject, email_content.message)
    logger.info("Confirmation email sent", data={"submitter_email": submitter_email})
