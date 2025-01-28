import os
import re
from pathlib import Path

from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_templates import (
    failed_file_upload_email,
    required_file_not_found_email,
    submission_processed_email,
    successful_file_upload_email,
    successful_validation_email,
    supplementary_distribution_not_found_email,
)
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.pipelineconfig.transform import get_transform_details
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_mimetype,
    get_submitter_email,
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
    # Validate the pipeline
    files_dir = Path(files_dir)
    validation_results = validate_pipeline_files(files_dir, pipeline_config)

    # Create notifier from webhook env var
    de_notifier = get_notifier()

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
    except Exception as err:
        logger.error(
            "Failed to create local data store from files directory",
            err,
            data={"files_directory": files_dir},
        )
        de_notifier.failure()
        raise err

    # Retrieve submitter email from manifest_dict and create email client from env var
    try:
        submitter_email = get_submitter_email(validation_results["manifest"])
        email_client = get_email_client()
        logger.info(
            "Submitter email received, email client created",
            data={"email_client": email_client},
        )
    except Exception as err:
        logger.error("Failed to create email client", err)
        de_notifier.failure()
        raise err

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
        except Exception as err:
            logger.error("Failed to retrieve Upload Service URL", err)
            de_notifier.failure()
            raise err

    # Get the transform function from pipeline config
    transform_function = get_transform_details(pipeline_config, "transform")
    # Get transform keyword arguments (kwargs) from pipeline config
    transform_kwargs = get_transform_details(pipeline_config, "transform_kwargs")
    logger.info(
        "Retrieved transform function and transform_kwargs from pipeline config",
        data={
            "transform_function": transform_function,
            "transform_kwargs": transform_kwargs,
            "input_file_paths": validation_results["input_files"],
        },
    )

    try:
        csv_path, metadata_path = transform_function(
            *validation_results["input_files"], **transform_kwargs
        )
        logger.info(
            "Transform function executed successfully",
            data={
                "transform_function": transform_function,
                "input_file_paths": validation_results["input_files"],
                "transform_kwargs": transform_kwargs,
                "csv_path": csv_path,
                "metadata_path": metadata_path,
            },
        )

    except Exception as err:
        logger.error(
            "Transform function execution failed",
            err,
            data={
                "transform_function": transform_function,
                "input_file_paths": validation_results["input_files"],
                "transform_kwargs": transform_kwargs,
                "pipeline_config": pipeline_config,
            },
        )
        de_notifier.failure()
        raise err

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
        except Exception as err:
            logger.error(
                "Failed to create UploadClient",
                err,
                data={"upload_url": upload_url},
            )
            de_notifier.failure()
            raise err

        try:
            # Upload CSV to Upload Service
            upload_client.upload_new(csv_path, "text/csv")
            logger.info(
                "CSV uploaded to Upload Service",
                data={
                    "csv_path": csv_path,
                    "upload_url": upload_url,
                },
            )
            email_content = successful_file_upload_email(csv_path.name)
            email_client.send(
                submitter_email, email_content.subject, email_content.message
            )
        except Exception as err:
            logger.error(
                "Failed to upload CSV file to Upload Service",
                err,
                data={
                    "csv_path": csv_path,
                    "upload_url": upload_url,
                },
            )
            de_notifier.failure()
            email_content = failed_file_upload_email(csv_path.name, str(err))
            email_client.send(
                submitter_email, email_content.subject, email_content.message
            )
            raise err

        # Check for supplementary distributions to upload
        supp_dist_patterns = get_matching_pattern(
            pipeline_config, "supplementary_distributions"
        )
        logger.info(
            "Retrieved supplementary distribution patterns from pipeline config",
            data={"supplementary_distribution_patterns": supp_dist_patterns},
        )

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

                # Create a directory to save supplementary distribution
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
                except Exception as err:
                    logger.error(
                        "Failed to upload supplementary distribution",
                        err,
                        data={
                            "supplementary_distribution": supp_dist_path,
                            "upload_url": upload_url,
                        },
                    )
                    de_notifier.failure()
                    email_content = failed_file_upload_email(
                        supp_dist_path.name, str(err)
                    )
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                    raise err

    email_content = submission_processed_email()
    email_client.send(submitter_email, email_content.subject, email_content.message)
    de_notifier.success()