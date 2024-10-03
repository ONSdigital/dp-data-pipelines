import os
import re
from pathlib import Path

from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_template_message import (
    file_not_found_email,
    supplementary_distribution_not_found_email,
)
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.pipelineconfig.transform import get_transform_details
from dpypelines.pipeline.shared.utils import get_email_client, get_submitter_email

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
    try:
        de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK"
        )
        logger.info("Notifier created", data={"notifier": de_notifier})
    except Exception as err:
        logger.error("Error occurred when creating notifier", err)
        raise err

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
            "Error occurred when creating local data store from files directory",
            err,
            data={"local_store_dir": files_dir},
        )
        de_notifier.failure()
        raise err

    try:
        manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")
        logger.info(
            "Got manifest.json dict output",
            data={"manifest_dict": manifest_dict},
        )
    except Exception as err:
        logger.error("Error occurred when getting manifest_dict", err)
        de_notifier.failure()
        raise err

    try:
        submitter_email = get_submitter_email(manifest_dict)
        logger.info(
            "Got submitter email",
            data={"submitter_email": submitter_email},
        )
    except Exception as err:
        logger.error(
            "Error occurred when getting submitter email",
            err,
            data={"manifest_dict": manifest_dict},
        )
        de_notifier.failure()
        raise err

    # Create email client from env var
    try:
        email_client = get_email_client()
        logger.info("Created email client", data={"email_client": email_client})
    except Exception as err:
        logger.error("Error occurred when creating email client", err)
        de_notifier.failure()
        raise err

    # Get Upload Service URL from environment variable
    try:
        upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
        assert (
            upload_url is not None and os.environ["SKIP_DATA_UPLOAD"] is not False
        ), "UPLOAD_SERVICE_URL environment variable not set."
        logger.info("Got Upload Service URL", data={"upload_url": upload_url})
    except Exception as err:
        logger.error("Error occurred when getting Upload Service URL", err)
        de_notifier.failure()
        raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = get_matching_pattern(pipeline_config, "required_files")
        logger.info(
            "Required file patterns retrieved from pipeline config",
            data={
                "required_file_patterns": required_file_patterns,
                "pipeline_config": pipeline_config,
            },
        )
    except Exception as err:
        logger.error(
            "Error occurred when getting required file patterns",
            err,
            data={
                "pipeline_config": pipeline_config,
            },
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                try:
                    raise FileNotFoundError(
                        f"Could not find file found matching pattern {required_file}"
                    )
                except FileNotFoundError as err:
                    email_content = file_not_found_email(required_file)
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                    # TODO add logging.error
                    logger.error(
                        "Error occurred when looking for required file",
                        err,
                        data={"required_file": required_file},
                    )
                    de_notifier.failure()
                    raise err
        except Exception as err:
            # files_in_directory = local_store.get_file_names()
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
            "Supplementary distribution patterns retrieved from pipeline config",
            data={"supplementary_distribution_patterns": supp_dist_patterns},
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Error occurred when getting supplementary distribution patterns",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each supplementary distribution
    for supp_dist_pattern in supp_dist_patterns:
        try:
            if not local_store.has_lone_file_matching(supp_dist_pattern):
                # Catch a trivial raise as we need the stack trace of the
                # error for the logger, so it needs to be a raised error.
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
                    # TODO add logging.error
                    logger.error(
                        "Error occurred when looking for supplementary distribution",
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
    input_file_paths = []
    try:
        transform_inputs = get_transform_details(pipeline_config, "transform_inputs")
        logger.info("Got transform inputs", data={"transform_inputs": transform_inputs})
    except Exception as err:
        logger.error(
            "Error when getting transform inputs from pipeline config",
            err,
            data={"pipeline_config": pipeline_config},
        )

    for pattern, sanity_checker in transform_inputs.items():
        try:
            input_file_path: Path = local_store.save_lone_file_matching(pattern)
            logger.info(
                "Saved input file that matches pattern",
                data={
                    "input_file_path": input_file_path,
                    "pattern": pattern,
                    "files_in_directory": files_in_directory,
                },
            )
        except Exception as err:
            logger.error(
                "Error occurred when looking for file matching pattern",
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
            "Got transform function from pipeline config",
            data={
                "transform_function": transform_function,
                "input_file_paths": input_file_paths,
            },
        )
    except Exception as err:
        logger.error(
            "Error occurred when getting transform function from pipeline config",
            err,
            data={"pipeline_config": pipeline_config},
        )
    # Get transform keyword arguments (kwargs) from pipeline config
    try:
        transform_kwargs = get_transform_details(pipeline_config, "transform_kwargs")
        logger.info(
            "Got transform kwargs from pipeline config",
            data={"transform_kwargs": transform_kwargs},
        )
    except Exception as err:
        logger.error(
            "Error occurred when getting transform kwargs from pipeline config",
            err,
            data={"pipeline_config": pipeline_config},
        )

    try:
        csv_path, metadata_path = transform_function(
            *input_file_paths, **transform_kwargs
        )
        logger.info(
            "Successfully ran transform function",
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
            "Error occurred when running transform function",
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

    # Allow DE's to skip the upload to s3 part of the pipeline while
    # developing code locally.
    skip_data_upload = os.environ.get("SKIP_DATA_UPLOAD", False)
    if skip_data_upload is not False:
        try:
            skip_data_upload = str_to_bool(skip_data_upload)
        except Exception as err:
            logger.error(
                "Unable to cast SKIP_DATA_UPLOAD to boolean",
                err,
                data={"value": skip_data_upload},
            )
            de_notifier.failure()
            raise err

    logger.info(
        "skip_data_upload set from SKIP_DATA_UPLOAD env var",
        data={"value": skip_data_upload},
    )

    if skip_data_upload is not True:

        # Upload output files to Upload Service
        try:
            # Create UploadClient from upload_url
            upload_client = UploadServiceClient(upload_url)
            logger.info(
                "UploadClient created from upload_url", data={"upload_url": upload_url}
            )
        except Exception as err:
            logger.error(
                "Error creating UploadClient", err, data={"upload_url": upload_url}
            )
            de_notifier.failure()
            raise err

        try:
            # Upload CSV to Upload Service
            upload_client.upload_new_csv(csv_path)
            logger.info(
                "Uploaded CSV to Upload Service",
                data={
                    "csv_path": csv_path,
                    "upload_url": upload_url,
                },
            )
        except Exception as err:
            logger.error(
                "Error uploading CSV file to Upload Service",
                err,
                data={
                    "csv_path": csv_path,
                    "upload_url": upload_url,
                },
            )
            de_notifier.failure()
            raise err

        # Check for supplementary distributions to upload
        if supp_dist_patterns:
            # Get list of all files in local store
            all_files = local_store.get_file_names()
            logger.info("Got all files in local store", data={"files": all_files})
            for supp_dist_pattern in supp_dist_patterns:
                # Get supplementary distribution filename matching pattern from local store
                supp_dist_matching_files = [
                    f for f in all_files if re.search(supp_dist_pattern, f)
                ]
                assert (
                    len(supp_dist_matching_files) == 1
                ), f"Error finding file matching pattern {supp_dist_pattern}: matching files are {supp_dist_matching_files}"

                # Create a directory to save supplementary distribution
                if not os.path.exists("supplementary_distributions"):
                    os.mkdir("supplementary_distributions")
                supp_dist_path = local_store.save_lone_file_matching(
                    supp_dist_pattern, "supplementary_distributions"
                )
                logger.info(
                    "Got supplementary distribution",
                    data={
                        "supplementary_distribution": supp_dist_path,
                        "file_extension": supp_dist_path.suffix,
                    },
                )
                # If the supplementary distribution is an XML file, upload to the Upload Service
                if supp_dist_path.suffix == ".xml":
                    try:
                        upload_client.upload_new_sdmx(supp_dist_path)
                        logger.info(
                            "Uploaded supplementary distribution",
                            data={
                                "supplementary_distribution": supp_dist_path,
                                "upload_url": upload_url,
                            },
                        )
                    except Exception as err:
                        logger.error(
                            "Error uploading SDMX file to Upload Service",
                            err,
                            data={
                                "supplementary_distribution": supp_dist_path,
                                "upload_url": upload_url,
                            },
                        )
                        de_notifier.failure()
                        raise err
                else:
                    raise NotImplementedError(
                        f"Uploading files of type {supp_dist_path.suffix} not supported."
                    )

    de_notifier.success()
