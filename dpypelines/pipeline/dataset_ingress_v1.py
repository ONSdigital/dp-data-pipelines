import os
import re
from pathlib import Path

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_templates import (
    submission_processed_email,
    successful_file_upload_email,
    successful_validation_email,
)
from dpypelines.pipeline.shared.error_handler_module import error_handler
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_mimetype,
    get_submitter_email,
)
from dpypelines.pipeline.utils import (
    get_notifier,
    get_value_from_metadata,
)
from dpypelines.pipeline.validate_ingest_files import (
    file_size_0,
    metadata_json_is_parseable,
)

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

    # Retrieve manifest.json as dict from local store
    try:
        manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")
    except Exception:
        error_handler(
            section="1.1",
            error="Failed to retrieve manifest.json",
            data=None,
            submitter_email="",
            enable_email=enable_email,
            enable_logs=enable_logs,
            enable_notification=enable_notification,
        )

    # Retrieve submitter email from manifest_dict and create email client from env var
    try:
        submitter_email = get_submitter_email(manifest_dict)
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

    # Validate existence of each file in the directory and that it is not empty
    for file in files_in_directory:

        filepath = os.path.join(files_dir, file)

        # Make sure file is not empty
        file_size_0(filepath, give_error=True)

        # Validate that metadata.json is parseable as JSON
        if "metadata.json" in filepath:
            metadata_json_is_parseable(filepath, give_error=True)

        email_content = successful_validation_email(file)
        email_client.send(submitter_email, email_content.subject, email_content.message)

    # Allow DE's to skip uploading to S3 while developing code locally.
    # Retrieve SKIP_DATA_UPLOAD value from environment variable
    skip_data_upload = os.environ.get("SKIP_DATA_UPLOAD", "False")
    skip_data_upload = str_to_bool(skip_data_upload)

    # Retrieve Upload Service and Dataset API URLs from environment variables
    if not skip_data_upload:
        try:
            upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
            assert (
                upload_url is not None
            ), "UPLOAD_SERVICE_URL environment variable not set"
            dataset_api_url = os.environ.get("DATASET_API_URL", None)
            assert (
                dataset_api_url is not None
            ), "DATASET_API_URL environment variable is not set"
        except Exception:
            error_handler(
                section="1.1",
                error="Failed to retrieve Upload Service/Dataset API URL",
                data=None,
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

    # Extract the patterns for required files from the pipeline configuration
    required_file_patterns = get_matching_pattern(pipeline_config, "required_files")
    logger.info(
        "Retrieved required file patterns from pipeline config",
        data={
            "required_file_patterns": required_file_patterns,
            "pipeline_config": pipeline_config,
        },
    )

    # Check that all required files are present in the local store
    for required_file in required_file_patterns:
        if not local_store.has_lone_file_matching(required_file):
            error_handler(
                section="1.1",
                error="Required file not found",
                data={
                    "required_file": required_file,
                    "required_file_patterns": required_file_patterns,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

    # Extract the patterns for supplementary distributions from the pipeline configuration
    supp_dist_patterns = get_matching_pattern(
        pipeline_config, "supplementary_distributions"
    )
    logger.info(
        "Retrieved supplementary distribution patterns from pipeline config",
        data={"supplementary_distribution_patterns": supp_dist_patterns},
    )

    # Check for the existence of each supplementary distribution
    for supp_dist_pattern in supp_dist_patterns:
        if not local_store.has_lone_file_matching(supp_dist_pattern):
            error_handler(
                section="1.1",
                error="Supplementary distribution not found.",
                data={
                    "supplementary_distribution": supp_dist_pattern,
                    "supplementary_distribution_patterns": supp_dist_patterns,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    if not skip_data_upload:
        # Upload output files to Upload Service
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
            for required_file_pattern in required_file_patterns:
                required_file_path = local_store.get_pathlike_of_file_matching(
                    required_file_pattern
                )
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
                    Path(required_file_pattern).name
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

        # Check for supplementary distributions to upload
        if supp_dist_patterns:
            # Get all files in local store
            all_files = local_store.get_file_names()

            for supp_dist_pattern in supp_dist_patterns:
                # Get supplementary distribution filename matching pattern from local store
                supp_dist_matching_files = [
                    f for f in all_files if re.search(supp_dist_pattern, f)
                ]
                assert (
                    len(supp_dist_matching_files) == 1
                ), f"Error finding file matching pattern {supp_dist_pattern}: matching files are {supp_dist_matching_files}"

                # Get filepath of supplementary distribution in local store
                supp_dist_path = local_store.get_pathlike_of_file_matching(
                    supp_dist_pattern
                )
                logger.info(
                    "Retrieved supplementary distribution",
                    data={
                        "supplementary_distribution": supp_dist_path,
                        "file_extension": supp_dist_path.suffix,
                    },
                )

                # Upload supplementary distribution to Upload Service
                try:
                    mimetype = get_mimetype(supp_dist_path.suffix)
                    if mimetype:
                        upload_client.upload_new(supp_dist_path, mimetype)
                    else:
                        raise NotImplementedError(
                            f"Uploading files of type {supp_dist_path.suffix} not supported."
                        )
                    logger.info(
                        "Supplementary distribution uploaded",
                        data={
                            "supplementary_distribution": supp_dist_path,
                            "upload_url": upload_url,
                        },
                    )
                    email_content = successful_file_upload_email(supp_dist_path.name)
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                except Exception:
                    error_handler(
                        section="1.1",
                        error="Failed to upload supplementary distribution",
                        data={
                            "supplementary_distribution": supp_dist_path,
                            "upload_url": upload_url,
                        },
                        submitter_email=submitter_email,
                        enable_email=enable_email,
                        enable_logs=enable_logs,
                        enable_notification=enable_notification,
                    )

        # Submit metadata to Dataset API
        try:
            metadata = local_store.get_lone_matching_json_as_dict("^metadata.json$")
            logger.info(
                "Retrieved metadata.json",
                data={"metadata": metadata},
            )
        except Exception as err:
            logger.error("Failed to retrieve metadata.json", err)
            de_notifier.failure()
            raise err

        # Submit metadata to Dataset API endpoint
        try:
            # 2423 TODO This is based on the understanding that the dataset_id will be the value associated with the dcterms:identifier predicate in metadata.json

            # Get dataset_id from metadata and create DatasetAPIClient
            dataset_id = get_value_from_metadata(metadata, "dcterms:identifier")
            dataset_api_client = DatasetAPIClient(dataset_api_url, dataset_id)

            # Check that the Dataset API endpoint exists
            # 2423 TODO do we need to handle the difference between a nonsense dataset_id (i.e. one that shouldn't exist) and a valid dataset_id that doesn't yet exist in the Dataset API? Or will this be done within the API?
            dataset_api_response = dataset_api_client.get_path()

            if dataset_api_response.status_code == 404:
                # 2423 TODO This doesn't actually print, because of how the error is handled in BaseHTTPClient._handle_request()
                print(
                    "Dataset ID does not exist in Dataset API - submit POST request to add new dataset"
                )
            elif dataset_api_response.status_code == 200:
                print(
                    "Dataset ID exists in Dataset API - submit PUT request to update existing dataset"
                )
            else:
                print("Unhandled status code")
        except Exception:
            error_handler(
                section="1.1",
                error="Error getting Dataset API path for given dataset_id",
                data={
                    "dataset_api_url": dataset_api_url,
                    "dataset_id": dataset_id,
                },
                submitter_email=submitter_email,
                enable_email=enable_email,
                enable_logs=enable_logs,
                enable_notification=enable_notification,
            )

    email_content = submission_processed_email()
    email_client.send(submitter_email, email_content.subject, email_content.message)
    de_notifier.success()
    return True
