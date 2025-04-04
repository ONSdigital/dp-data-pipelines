import json
import os
import random
import string
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
from dpypelines.pipeline.messages.email_templates import (
    failed_metadata_submission,
    submission_processed_email,
    successful_file_upload_email,
    successful_metadata_submission,
)
from dpypelines.pipeline.messages.notification import (
    PipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.messages.utils import (
    get_email_client,
    get_local_time,
    get_mimetype,
)
from dpypelines.pipeline.validate_pipeline import validate_pipeline_files

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


def copy_s3_processing_folder_to_destination_folder(
    s3_object_name: str,
    decompressed_file_dir: Path,
    s3_processing_folder: str,
    destination: str,
) -> str:
    """
    Copy all files in S3 "processing" folder to S3 destination folder.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)
    s3_destination_folder = f"{destination}/{datetime.now().strftime('%y-%m-%dT%H-%M')}-{decompressed_file_dir.parts[-1]}"
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Copy unzipped files from S3 "processing" folder to destination folder
    for file_path in decompressed_file_dir.rglob("*"):
        s3_client.copy_object(
            Bucket=bucket_name,
            Key=f"{s3_destination_folder}/{file_path.name}",
            CopySource={
                "Bucket": bucket_name,
                "Key": f"{s3_processing_folder}/{file_path.name}",
            },
        )

    # Copy original zip file from S3 "processing" folder to destination folder
    s3_client.copy_object(
        Bucket=bucket_name,
        Key=f"{s3_destination_folder}/{object_key}",
        CopySource={
            "Bucket": bucket_name,
            "Key": f"{s3_processing_folder}/{object_key}",
        },
    )
    logger.info(
        "Decompressed files and original zip submission copied to S3 'processed' folder",
        data={"s3_processed_folder": s3_destination_folder},
    )

    return s3_destination_folder


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
            "release_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "edition_title": edition.get("dcterms:title", None),
            # Tier 0 metadata standards - required with output
            "title": metadata.get("dcterms:title", None),
            "description": metadata.get("dcterms:description", None),
            "next_release": metadata.get("TBC:nextRelease", None),
            "themes": metadata.get("dcat:theme", None),
            # # Additional properties (from swagger.yaml)
            "alerts": edition.get("TBC:alerts", None),
            "usage_notes": edition.get("TBC:usage_notes", None),
            # TODO
            # "quality_designation": edition.get("TBC:quality_designation", None),
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
                "download_url": distribution["download_url"],
                # TODO Calculate byte_size during processing
                "byte_size": 0,
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


def check_dataset_type_is_static(dataset_api_client: DatasetAPIClient) -> bool:
    get_dataset_result = dataset_api_client.get(
        f"{dataset_api_client.dataset_api_url}/{dataset_api_client.dataset_path}",
        headers=dataset_api_client.token_auth.get_auth_header(),
    )
    if get_dataset_result.status_code == 200:
        dataset_result = json.loads(get_dataset_result.text)
        current_dataset = dataset_result.get("current", None)
        if current_dataset:
            dataset_type = current_dataset.get("type", None)
            if dataset_type and dataset_type == "static":
                logger.info("Dataset type is static")
                return True
            elif dataset_type and dataset_type != "static":
                logger.info("Dataset type is not static")
                return False
            else:
                logger.info("Dataset type not specified")
                return False
        else:
            raise KeyError("'current' not found in dataset_result keys")
    else:
        get_dataset_result.raise_for_status()


def upload_metadata(metadata, email_client, submitter_email) -> bool:
    """Upload metadata to the Dataset API and send notifications."""
    dataset_api_url = os.environ.get("DATASET_API_URL")
    if not dataset_api_url:
        msg = "Required environment variable(s) not set: DATASET_API_URL"
        raise EnvironmentError(msg)

    # Generate POST request body from metadata
    dataset_path, edition_path, request_body = get_post_request_values_from_metadata(
        metadata
    )
    dataset_api_client = DatasetAPIClient(dataset_api_url, dataset_path, edition_path)

    # Upload metadata only if the dataset type is "static"
    if check_dataset_type_is_static(dataset_api_client):
        # Verify that the relevant Dataset API endpoint exists
        dataset_api_get_path_response = dataset_api_client.get_path()

        # If the endpoint exists, send POST request
        if dataset_api_get_path_response.status_code == 200:
            logger.info(
                "Dataset API endpoint exists",
                data={"dataset_api_endpoint": dataset_api_client.full_url},
            )
            # 2809 Currently, if the `state` of a dataset version is anything other than `published` (e.g. `associated`), an error occurs when trying to submit a new version: 400 Bad Request: '{"error":"cannot create new version when an unpublished version already exists","unpublished_version":1}'

            # 2809 L417 shows the previous method - this submits a POST request to dataset_api_client.full_url (i.e. dataset_api_url/dataset_path/editions/edition_path/versions):
            # dataset_api_client.post_json(request_body)

            # 2809 Fran recommended generating a new edition_id for every request, and this allows the POST request to succeed, but may not be a viable solution (see below)
            chars = string.ascii_lowercase + string.digits
            random_edition_id = "".join(random.choices(chars, k=8))
            dataset_api_client.post(
                f"{dataset_api_url}/{dataset_path}/editions/{random_edition_id}/versions",
                headers=dataset_api_client.token_auth.get_auth_header(),
                json=request_body,
                verify=True,
            )
            # # 2809 The issue with this approach is that a subsequent GET request to dataset_api_url/dataset_path/editions/random_edition_id/versions returns a 404 ('edition not found'):
            # get_versions_res = dataset_api_client.get(
            #     f"{dataset_api_url}/{dataset_path}/editions/{random_edition_id}/versions",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            # # 2809 get_editions_res.status code = 200 but random_edition_id doesn't appear in the list of editions returned
            # get_editions_res = dataset_api_client.get(
            #     f"{dataset_api_url}/{dataset_path}/editions",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            # # 2809 get_edition_res.status_code = 404 ('edition not found')
            # get_edition_res = dataset_api_client.get(
            #     f"{dataset_api_url}/{dataset_path}/editions/{random_edition_id}",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )

            # # 2809 However, the submitted edition can be accessed by port forwarding to a Dataset API publishing instance and submitting the request to {publishing_instance_url}/{dataset_path}/editions/{random_edition_id}:

            # publishing_instance_url = "http://localhost:17892/datasets"
            # get_dataset_res = dataset_api_client.get(
            #     f"{publishing_instance_url}/{dataset_path}",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            # get_editions_res = dataset_api_client.get(
            #     f"{publishing_instance_url}/{dataset_path}/editions",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            # get_edition_res = dataset_api_client.get(
            #     f"{publishing_instance_url}/{dataset_path}/editions/{random_edition_id}",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            # get_versions_res = dataset_api_client.get(
            #     f"{publishing_instance_url}/{dataset_path}/editions/{random_edition_id}/versions",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            # get_version_res = dataset_api_client.get(
            #     f"{publishing_instance_url}/{dataset_path}/editions/{random_edition_id}/versions/1",
            #     headers=dataset_api_client.token_auth.get_auth_header(),
            # )
            logger.info(
                "Metadata submitted to Dataset API endpoint",
                data={"dataset_api_endpoint": dataset_api_client.full_url},
            )
            email_content = successful_metadata_submission(dataset_path)
            email_client.send(
                submitter_email, email_content.subject, email_content.message
            )
            return True
        else:
            dataset_api_get_path_response.raise_for_status()
    else:
        msg = "Dataset type is not static: metadata not submitted to Dataset API"
        email_content = failed_metadata_submission(dataset_path, error_info=msg)
        email_client.send(submitter_email, email_content.subject, email_content.message)
        logger.info(msg)
        return False


def upload_files(files_to_upload, email_client, submitter_email):
    """Upload files and send notifications."""
    upload_url = os.environ.get("UPLOAD_SERVICE_URL")
    if not upload_url:
        err_msg = "Required environment variable(s) not set: UPLOAD_SERVICE_URL."
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
        email_content = successful_file_upload_email(Path(required_file_path).name)
        email_client.send(submitter_email, email_content.subject, email_content.message)
        logger.info(
            "File upload notification email sent",
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
