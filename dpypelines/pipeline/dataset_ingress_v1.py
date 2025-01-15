import os
import re
from pathlib import Path

from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_templates import (
    failed_file_upload_email,
    failed_validation_email,
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

    # DIS-2334 TODO: Create a helper function for error handling to reduce repetition
    
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
        logger.info(
            "Retrieved manifest.json",
            data={"manifest_dict": manifest_dict},
        )
    except Exception as err:
        logger.error("Failed to retrieve manifest.json", err)
        de_notifier.failure()
        raise err

    # Retrieve submitter email from manifest_dict
    try:
        submitter_email = get_submitter_email(manifest_dict)
        logger.info(
            "Retrieved submitter email",
            data={"submitter_email": submitter_email},
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve submitter email",
            err,
            data={"manifest_dict": manifest_dict},
        )
        de_notifier.failure()
        raise err

    # Create email client from env var
    try:
        email_client = get_email_client()
        logger.info(
            "Email client created",
            data={"email_client": email_client},
        )
    except Exception as err:
        logger.error("Failed to create email client", err)
        de_notifier.failure()
        raise err

    # Validate existence of each file in the directory and that it is not empty
    # DIS-2334 TODO: Extract file validation into separate functions
    for file in files_in_directory:
        try:
            if not local_store.has_lone_file_matching(file):
                try:
                    # Catch a trivial raise as we need the stack trace of the error for the logger, so it needs to be a raised error.
                    raise FileNotFoundError(f"{file} does not exist")
                except FileNotFoundError as err:
                    email_content = required_file_not_found_email(file)
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                    logger.error("Input file not found", err, data={"file": file})
                    de_notifier.failure()
                    raise err

            filepath = os.path.join(files_dir, file)
            if file_size_0(filepath):
                try:
                    raise ValueError(f"'{file}' is empty")
                except ValueError as err:
                    email_content = failed_validation_email(
                        file, f"File '{file}' is empty"
                    )
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                    logger.error("Input file is empty", err, data={"file": file})
                    de_notifier.failure()
                    raise err

            # Validate that metadata.json is parseable as JSON
            if "metadata.json" in filepath:
                if metadata_json_is_parseable(filepath):
                    logger.info(
                        "metadata.json is parseable as JSON", data={"file": file}
                    )
                else:
                    try:
                        raise ValueError("metadata.json is not parseable")
                    except ValueError as err:
                        email_content = failed_validation_email(
                            file, "metadata.json is not parseable as JSON"
                        )
                        email_client.send(
                            submitter_email,
                            email_content.subject,
                            email_content.message,
                        )
                        logger.error("metadata.json is not parseable as JSON", err)
                        de_notifier.failure()
                        raise err
            logger.info("File exists and is not empty", data={"file": file})
            email_content = successful_validation_email(file)
            email_client.send(
                submitter_email, email_content.subject, email_content.message
            )
        except Exception as err:
            logger.error(
                "Failed to validate input file",
                err,
                data={"local_store_dir": files_dir, "file": file},
            )
            de_notifier.failure()
            raise err

    # DIS-2334 TODO: Create a helper function for environment variable retrieval and parsing
    # Allow DE's to skip uploading to S3 while developing code locally.
    # Retrieve SKIP_DATA_UPLOAD value from environment variable
    skip_data_upload = os.environ.get("SKIP_DATA_UPLOAD", "False")
    try:
        skip_data_upload = str_to_bool(skip_data_upload)
    except Exception as err:
        logger.error(
            "Failed to cast SKIP_DATA_UPLOAD to boolean",
            err,
            data={"value": skip_data_upload},
        )
        de_notifier.failure()
        raise err
    logger.info(
        "skip_data_upload set from SKIP_DATA_UPLOAD env var",
        data={"value": skip_data_upload},
    )

    # Retrieve Upload Service URL from environment variable
    if not skip_data_upload:
        try:
            upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
            assert (
                upload_url is not None
            ), "UPLOAD_SERVICE_URL environment variable not set"
            logger.info(
                "Retrieved Upload Service URL",
                data={"upload_url": upload_url},
            )
        except Exception as err:
            logger.error("Failed to retrieve Upload Service URL", err)
            de_notifier.failure()
            raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = get_matching_pattern(pipeline_config, "required_files")
        logger.info(
            "Retrieved required file patterns from pipeline config",
            data={
                "required_file_patterns": required_file_patterns,
                "pipeline_config": pipeline_config,
            },
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve required file patterns from pipeline config",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    # DIS-2334 TODO: Extract required file checking into a separate function/block
    # DIS-2334 TODO: unnecessary complexity with  sending email we shouldmodulise the emailclient code
    # Check that all required files are present in the local store
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                try:
                    raise FileNotFoundError(
                        f"No file found matching pattern {required_file}"
                    )
                except FileNotFoundError as err:
                    email_content = required_file_not_found_email(required_file)
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                    logger.error(
                        "Required file not found",
                        err,
                        data={"required_file": required_file},
                    )
                    de_notifier.failure()
                    raise err
        except Exception as err:
            logger.error(
                "Error occurred when looking for required file",
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
    try:
        supp_dist_patterns = get_matching_pattern(
            pipeline_config, "supplementary_distributions"
        )
        logger.info(
            "Retrieved supplementary distribution patterns from pipeline config",
            data={"supplementary_distribution_patterns": supp_dist_patterns},
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Failed to retrieve supplementary distribution patterns",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each supplementary distribution
    # DIS-2334 TODO: potentially we can create a helper function for sending emails  and checks if email is sent or not
    for supp_dist_pattern in supp_dist_patterns:
        try:
            if not local_store.has_lone_file_matching(supp_dist_pattern):
                try:
                    raise FileNotFoundError(
                        f"No file found matching pattern {supp_dist_pattern}"
                    )
                except FileNotFoundError as err:
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
                        },
                    )
                    de_notifier.failure()
                    raise err
        except Exception as err:
            logger.error(
                "Error occurred when looking for supplementary distribution",
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

    # Get the transform inputs from the pipeline_config and run the specified sanity checker for it
    # DIS-2334 TODO: Extract transform input retrieval and sanity checking into separate functions
    # DIS-2334 TODO: this would remove complexity around the code
    input_file_paths = []
    try:
        transform_inputs = get_transform_details(pipeline_config, "transform_inputs")
        logger.info(
            "Retrieved transform inputs",
            data={"transform_inputs": transform_inputs},
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve transform inputs from pipeline config",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    for pattern, sanity_checker in transform_inputs.items():
        try:
            input_file_path: Path = local_store.get_pathlike_of_file_matching(pattern)
            logger.info(
                "Retrieved input file that matches pattern",
                data={
                    "input_file_path": input_file_path,
                    "pattern": pattern,
                    "files_in_directory": files_in_directory,
                },
            )
        except Exception as err:
            logger.error(
                "Failed to retrieve input file matching pattern",
                err,
                data={
                    "pattern": pattern,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )

            de_notifier.failure()
            raise err

        try:
            sanity_checker(input_file_path)
            logger.info(
                "Sanity check run on input file path.",
                data={
                    "sanity_checker": sanity_checker,
                    "input_file_path": input_file_path,
                },
            )
        except Exception as err:
            logger.error(
                "Error occurred when running sanity checker on input file path.",
                err,
                data={
                    "input_file_path": input_file_path,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )

            de_notifier.failure()
            raise err

        input_file_paths.append(input_file_path)

    # Get the transform function from pipeline config
    try:
        transform_function = get_transform_details(pipeline_config, "transform")
        logger.info(
            "Retrieved transform function from piepline config",
            data={
                "transform_function": transform_function,
                "input_file_paths": input_file_paths,
            },
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve transform function from pipeline config",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    # Get transform keyword arguments (kwargs) from pipeline config
    try:
        transform_kwargs = get_transform_details(pipeline_config, "transform_kwargs")
        logger.info(
            "Retrieved transform kwargs  from pipeline cofig",
            data={"transform_kwargs": transform_kwargs},
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve transform kwargs",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    try:
        csv_path, metadata_path = transform_function(
            *input_file_paths, **transform_kwargs
        )
        logger.info(
            "Transform function executed successfully",
            data={
                "transform_function": transform_function,
                "input_file_paths": input_file_paths,
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
                "input_file_paths": input_file_paths,
                "transform_kwargs": transform_kwargs,
                "pipeline_config": pipeline_config,
            },
        )
        de_notifier.failure()
        raise err

    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    if not skip_data_upload:
        # Upload output files to Upload Service
        try:
            # Create UploadClient from upload_url
            upload_client = UploadServiceClient(upload_url)
            logger.info(
                "UploadClient created",
                data={"upload_url": upload_url},
            )
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
        
        # DIS-2334 TODO: Extract supplementary distribution upload into a separate function/block
        # DIS-2334 TODO: we can potenitally break this code down further which could remove complexity
        # Check for supplementary distributions to upload
        if supp_dist_patterns:
            # Get all files in local store
            all_files = local_store.get_file_names()
            logger.info(
                "Retrieved all files in local store",
                data={"files": all_files},
            )
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

    # DIS-2334 TODO: Extract submission processing into a separate function
    email_content = submission_processed_email()
    email_client.send(submitter_email, email_content.subject, email_content.message)
    de_notifier.success()