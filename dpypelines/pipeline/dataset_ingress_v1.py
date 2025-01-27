import os
import re

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
from dpypelines.pipeline.shared.transforms.process_tranform_module import (
    process_transform,
)
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_mimetype,
    get_submitter_email,
)
from dpypelines.pipeline.utils import get_notifier
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

    # Retrieve manifest.json as dict from local store
    try:
        manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")
    except Exception as err:
        logger.error("Failed to retrieve manifest.json", err)
        de_notifier.failure()
        raise err

    # Retrieve submitter email from manifest_dict and create email client from env var
    try:
        submitter_email = get_submitter_email(manifest_dict)
        email_client = get_email_client()
        logger.info(
            "Submitter email received, email client created",
            data={"email_client": email_client},
        )
    except Exception as err:
        logger.error("Failed to create email client", err)
        de_notifier.failure()
        raise err

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
    skip_data_upload = os.environ.get("SKIP_DATA_UPLOAD", "True")
    skip_data_upload = str_to_bool(skip_data_upload)

    skip_data_upload = True
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
            email_content = required_file_not_found_email(required_file)
            email_client.send(
                submitter_email, email_content.subject, email_content.message
            )
            err = FileNotFoundError(f"No file found matching pattern {required_file}")
            logger.error(
                "Required file not found",
                err,
                data={
                    "required_file": required_file,
                    "required_file_patterns": required_file_patterns,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )
            de_notifier.failure()
            raise err

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
        print(f"SUPP_PATTERN {supp_dist_pattern}")
        if not local_store.has_lone_file_matching(supp_dist_pattern):
            err = FileNotFoundError(
                f"No file found matching pattern {supp_dist_pattern}"
            )
            email_content = supplementary_distribution_not_found_email(
                supp_dist_pattern
            )
            email_client.send(
                submitter_email, email_content.subject, email_content.message
            )
            logger.error(
                "Supplementary distribution not found.",
                err,
                data={
                    "supplementary_distribution": supp_dist_pattern,
                    "supplementary_distribution_patterns": supp_dist_patterns,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )
            de_notifier.failure()
            raise err

    csv_path, metadat_path = process_transform(
        local_store, pipeline_config, files_in_directory, de_notifier
    )
    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    if not skip_data_upload:
        # Upload output files to Upload Service
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
