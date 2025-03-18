import os
import shutil
import zipfile
from pathlib import Path
from typing import Union

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


def clean_directory(directory: Union[str, Path]) -> None:
    """
    Delete all files and subdirectories in the given directory.

    Args:
        directory (Union[str, Path]): The path to the directory to clean.
    """
    directory = Path(directory)
    if not directory.exists():
        # If the directory doesn't exist, nothing to clean.
        return

    for item in directory.iterdir():
        try:
            if item.is_file() or item.is_symlink():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
            logger.info("Deleted item", data={"item": str(item)})
        except Exception as err:
            logger.error("Failed to delete item", err, data={"item": str(item)})
            raise err


def delete_subfolders(folder_path: Union[str, Path]) -> None:
    """
    Delete all subfolders within the given folder.

    Args:
        folder_path (Union[str, Path]): The path to the folder whose subfolders should be deleted.
    """
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"The folder {folder} does not exist.")

    for item in folder.iterdir():
        if item.is_dir():
            try:
                shutil.rmtree(item)
                print(f"Deleted folder: {item}")
            except Exception as e:
                print(f"Error deleting folder {item}: {e}")


def download_zip_file(s3_object_name: str) -> Path:
    """
    Downloads a zip file from S3 into the 'input' folder and returns the local file path.
    """
    input_dir = Path("input")
    input_dir.mkdir(parents=True, exist_ok=True)
    zip_filename = os.path.basename(s3_object_name)  # e.g., e2e.zip
    local_zip_path = input_dir / zip_filename

    bucket_name = s3_object_name.split("/")[0]
    object_key = "/".join(s3_object_name.split("/")[1:])
    profile_name = os.environ.get("AWS_PROFILE")
    client = _get_s3_client(profile_name)
    with open(local_zip_path, "wb") as f:
        client.download_fileobj(bucket_name, object_key, f)
    logger.info("Downloaded zip file", data={"local_zip_path": str(local_zip_path)})
    return local_zip_path


def decompress_zip_file(zip_path: Path, dest_folder: Union[str, Path] = "processing"):
    """
    Decompress the given zip file into the specified destination folder.
    After extraction, if the files are not contained within a subfolder,
    move them into a folder named as the zip file (without its extension).
    """
    dest_folder = Path(dest_folder)
    dest_folder.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(dest_folder)

    # List items in the destination folder
    extracted_items = list(dest_folder.iterdir())
    # If there is not exactly one directory, assume the files weren't extracted into a subfolder
    if not (len(extracted_items) == 1 and extracted_items[0].is_dir()):
        new_folder = dest_folder / zip_path.stem
        new_folder.mkdir(exist_ok=True)
        for item in extracted_items:
            shutil.move(str(item), new_folder)

    logger.info(
        "Decompressed zip file",
        data={"zip_path": str(zip_path), "dest_folder": str(dest_folder)},
    )


def move_extracted_folder(
    zip_filename: str,
    src_dir: Union[str, Path] = "processing",
    dest_dir: Union[str, Path] = "processed",
):
    """
    Move the extracted folder (with name matching the zip file name without extension)
    from the src_dir to the dest_dir. If a folder with the same name already exists in dest_dir,
    it will be removed first.
    """
    folder_name = Path(zip_filename).stem  # e.g., 'e2e' from 'e2e.zip'
    src_dir = Path(src_dir)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    src_folder = src_dir / folder_name
    dest_folder = dest_dir / folder_name

    if src_folder.exists() and src_folder.is_dir():
        # Remove the destination folder if it exists
        if dest_folder.exists():
            shutil.rmtree(dest_folder)
            logger.info(
                "Existing folder removed from processed directory",
                data={"folder": str(dest_folder)},
            )
        shutil.move(str(src_folder), str(dest_folder))
        logger.info(
            "Moved extracted folder",
            data={"folder": folder_name, "dest_folder": str(dest_folder)},
        )
    else:
        err_msg = f"Expected folder '{folder_name}' not found in {src_dir}."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)


def process_zip_file(s3_object_name: str):
    """
    Download a zip file from S3 into the 'input' folder, decompress it into 'processing',
    move the extracted folder (whose name matches the zip file name without extension)
    into 'processed', and then verify that the resulting folder contains files.

    Returns:
        LocalDirectoryStore: A local store representing the processed folder.
    """

    # Step 1: Download the zip file.
    local_zip_path = download_zip_file(s3_object_name)

    # Step 2: Decompress the zip file into the 'processing' folder.
    decompress_zip_file(local_zip_path, dest_folder="processing")

    # Delete zip file
    clean_directory("input")

    extracted_folder = Path("processing") / local_zip_path.stem
    bucket_name = s3_object_name.split("/")

    for file_path in extracted_folder.rglob("*"):
        if (
            file_path.is_file()
            and str(file_path).endswith(".json")
            or str(file_path).endswith(".csv")
        ):
            relative_path = file_path.relative_to("processing")
            object_name = f"{bucket_name[0]}/processing/{relative_path.as_posix()}"
            upload_local_file_to_s3(
                file_path, object_name, profile_name=os.environ.get("AWS_PROFILE")
            )

    delete_subfolders("processing/" + local_zip_path.stem)

    # Step 4: Validate that the decompressed (and moved) folder contains files.
    folder_name = Path(local_zip_path.name).stem  # e.g., 'e2e' from 'e2e.zip'
    processed_folder = Path("processing") / folder_name
    local_store = LocalDirectoryStore(processed_folder)
    files = local_store.get_file_names()
    if not files:
        err_msg = f"Decompressed directory 'input' is empty for s3_object: {s3_object_name}. Available files: {files}"
        raise FileNotFoundError(err_msg)

    logger.info(
        "S3 zip object processed successfully",
        data={
            "s3_object_name": s3_object_name,
            "processed_folder": str(processed_folder),
        },
    )
    return local_store


def validate_pipeline(files_dir: Path, pipeline_config: dict):
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


def upload_metadata(local_store, email_client, submitter_email):
    """Upload metadata and send notifications."""
    dataset_api_url = os.environ.get("DATASET_API_URL")
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
    upload_url = os.environ.get("UPLOAD_SERVICE_URL")
    dataset_api_url = os.environ.get("DATASET_API_URL")
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
