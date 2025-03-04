import os
from pathlib import Path
from datetime import datetime
from typing import Optional

import pytz
from git import Repo
from dpytools.email.ses.client import SesClient
from dpytools.utilities.utilities import str_to_bool
from email_validator import EmailNotValidError, validate_email
from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.messages.email_templates import (
    submission_processed_email,
    successful_file_upload_email,
    successful_metadata_submission,
)
from dpypelines.pipeline.messages.notification import (
    PipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.validate_pipeline import validate_pipeline_files

logger = DpLogger("data-ingress-pipelines")

MIMETYPES = {
    ".csv": "text/csv",
    ".xml": "application/xml",
    ".json": "application/json",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csdb": "application/octet-stream",
}

class NopEmailClient:
    def send(self, *args, **kwargs):
        print("Email feature is turned off. No email was sent.")


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
        logger.error("Error occurred when creating notifier", err)
        raise err


def get_email_client():
    """
    Creates an email client object to be used for sending notification/error report emails.
    """
    emails_disabled = os.environ.get("DISABLE_EMAILS", "True")
    emails_disabled = str_to_bool(emails_disabled)

    if emails_disabled:
        return NopEmailClient()

    ses_email_identity = os.environ["SES_EMAIL_IDENTITY"]
    email_client = SesClient(ses_email_identity, "eu-west-2")

    return email_client


def get_submitter_email(manifest_dict: dict) -> str:
    """
    This function returns the submitter email from the provided manifest_dict (which is generated from the manifest.json file)
    """

    # Temporary email address for testing purposes
    # Needs to be updated once we know where the submitter email can be extracted from
    submitter_email = manifest_dict["fileAuthorEmail"]

    if manifest_dict["manifestVersion"] != 1:
        raise ValueError(
            f'The manifest version does not match required version(which should be 1) suppllied version: {manifest_dict["manifestVersion"]}.'
        )

    if submitter_email is None:
        raise NotImplementedError("Submitter email address not found.")

    try:
        validate_email(submitter_email)
    except EmailNotValidError as e:
        raise ValueError(f"Invalid email address: {submitter_email}. Error: {str(e)}")

    return submitter_email


def get_local_time():
    """
    Utility function for retrieving a string of the date/time.
    """
    # Get the timezone object for London
    tz_London = pytz.timezone("Europe/London")

    # Get the current time in London
    datetime_London = datetime.now(tz_London)

    # Format London date time into hours, minutes, and seconds
    formatted_datetime_London = datetime_London.strftime("%H:%M:%S")

    # Format the time as a string and print it
    return formatted_datetime_London


def get_mimetype(file: str) -> Optional[str]:
    # TODO Set default to "application/octet-stream"?
    return MIMETYPES.get(file, None)


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
        logger.error(
            "Error getting POST request values from metadata",
            err,
            data={"metadata": metadata},
        )
        raise err


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
        logger.error(
            "Error getting details of distributions for Dataset API request",
            err,
            data={"distributions": dcat_distributions},
        )
        raise err


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


def decompress_file(s3_object_name):
    """Decompress the file to the local directory."""

    local_store = LocalDirectoryStore(s3_object_name)
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


def validate_pipeline(files_dir: Path, pipeline_config: dict):
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


def upload_metadata(local_store, email_client, submitter_email):
    """Upload metadata and send notifications."""
    dataset_api_url = os.environ.get("DATASET_API_URL")
    if not dataset_api_url:
        err_msg = (
            f"Required environment variable(s) not set: "
            f"DATASET_API_URL: {dataset_api_url}."
        )
        logger.error(err_msg)
        raise EnvironmentError(err_msg)

    metadata = local_store.get_lone_matching_json_as_dict("^metadata.json$")
    if not metadata:
        err_msg = "metadata.json not found in the local store."
        logger.error(err_msg)
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
    for required_file_path in validation_results["config_files"]:
        mimetype = get_mimetype(Path(required_file_path).suffix)
        if not mimetype:
            err_msg = f"Uploading file type {Path(required_file_path).suffix} not supported for file: {required_file_path}."
            logger.error(err_msg)
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
        logger.error(err_msg)
        raise ValueError(err_msg)
    email_client.send(submitter_email, email_content.subject, email_content.message)
    logger.info("Confirmation email sent", data={"submitter_email": submitter_email})


def get_commit_id() -> str:
    """
    Gets the current commit ID from the repository.
    """
    try:
        repo = Repo()
    except Exception:
        environment = os.environ["ENVIRONMENT"]
        if not os.path.exists("/tmp/dp-data-pipelines"):
            repo = Repo.clone_from(
                "https://github.com/ONSdigital/dp-data-pipelines.git",
                "/tmp/dp-data-pipelines",
                branch=environment,
            )
        else:
            repo = Repo("/tmp/dp-data-pipelines")

    return str(repo.head.commit)


def get_environment() -> str:
    """
    Gets the current environment.
    """
    try:
        repo = Repo()
        heads = repo.heads
        if "sandbox" in str(heads):
            return "sandbox"
        elif "staging" in str(heads):
            return "staging"
        elif "production" in str(heads):
            return "production"
        else:
            return "Environment is unknown"

    except Exception:
        # found in env variables
        environment = os.environ["ENVIRONMENT"]
        return environment