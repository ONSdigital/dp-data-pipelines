import os
from pathlib import Path
import re

from dpytools.http.upload import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared.email_template_message import (
    file_not_found_email,
    submission_processed_email,
    supplementary_distribution_not_found_email,
)
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig import matching
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_florence_access_token,
    get_submitter_email,
)

logger = DpLogger("data-ingress-pipeline")


def dataset_ingress_v1(files_dir: str, pipeline_config: dict):
    """
    Version one of the dataset ingress pipeline.

    files_dir: Path to the directory where the input
    files for this pipeline are located.

    Args:
        files_dir (str): Path to the directory where the input files for this pipeline are located.
        pipeline_config (dict): Dictionary of configuration details required to run the pipeline (determined by dataset id)

    Raises:
        ValueError: If required files, supplementary distributions, or pipeline configuration are not found in the input directory.
        Exception: If any other unexpected error occurs.
    """
    # Create notifier from webhook env var
    try:
        de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK"
        )
        logger.info("Created notifier instance", data={"notifier": de_notifier})
    except Exception as err:
        logger.error("Error occurred while attempting to create notifier instance", err)
        raise err

    try:
        submitter_email = get_submitter_email()
        logger.info(
            "Successfully got submitter email",
            data={"submitter_email": submitter_email},
        )
    except Exception as err:
        logger.error("Error occurred while attempting to get submitter email", err)
        de_notifier.failure()
        raise err

    # Create email client from env var
    try:
        email_client = get_email_client()
        logger.info(
            "Created email client instance", data={"email_client": email_client}
        )
    except Exception as err:
        logger.error(
            "Error occurred while attempting to create email client instance", err
        )
        de_notifier.failure()
        raise err

    # just throw out an email to see if it works
    try:
        email_content = submission_processed_email()
        email_client.send(submitter_email, email_content.subject, email_content.message)
        logger.info(
            "Successfully sent email",
            data={"submitter_email": submitter_email, "email_content": email_content},
        )
    except Exception as err:
        logger.error("Error occurred while attempting to send email", err)
        de_notifier.failure()
        raise Exception(message.unexpected_error("Failed to send email", err)) from err

    # Get Upload Service URL from environment variable
    try:
        upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
        assert (
            upload_url is not None
        ), "UPLOAD_SERVICE_URL environment variable not set."
        logger.info("Got Upload Service URL", data={"upload_url": upload_url})
    except Exception as err:
        logger.error("Error getting Upload Service URL", err)
        de_notifier.failure()
        raise err
    # Get Florence access token from environment variable
    try:
        florence_access_token = get_florence_access_token()
        assert (
            florence_access_token is not None
        ), "FLORENCE_TOKEN environment variable not set."
        logger.info("Got Florence access token")
    except Exception as err:
        logger.error("Error getting Florence access token", err)
        de_notifier.failure()
        raise err
    # Attempt to access the local data store
    try:
        local_store = LocalDirectoryStore(files_dir)
        files_in_directory = local_store.get_file_names()
        logger.info(
            "Local data store successfully instansiated",
            data={
                "local_store_dir": files_dir,
                "files_in_directory": files_in_directory,
            },
        )
    except Exception as err:
        logger.error(
            f"Failed to access local data at {files_dir}",
            err,
            data={"local_store_dir": files_dir},
        )
        de_notifier.failure()
        raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
        logger.info(
            "Required file patterns retrieved from pipeline configuration",
            data={"required_file_patterns": required_file_patterns},
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Failed to get required files pattern",
            err,
            data={
                "pipeline_config": pipeline_config,
                "files_in_directory": files_in_directory,
            },
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):

                email_content = file_not_found_email(required_file)
                email_client.send(
                    submitter_email, email_content.subject, email_content.message
                )
                logger.info(
                    "Sent email to submitter: ",
                    submitter_email,
                    " about missing required file: ",
                    required_file,
                )
                de_notifier.failure()
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            logger.error(
                f"Error while looking for required file {required_file}",
                err,
                data={
                    "required_file": required_file,
                    "required_file_patterns": required_file_patterns,
                    "files_in_directory": files_in_directory,
                },
            )
            de_notifier.failure()
            raise err

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supp_dist_patterns = matching.get_supplementary_distribution_patterns(
            pipeline_config
        )
        logger.info(
            "Successfully retrieved supplementary distribution patterns from pipeline config",
            data={"supplementary_distribution_pattenrs": supp_dist_patterns},
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Failed to get supplementary distribution patterns",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each supplementary distribution
    for supp_dist_pattern in supp_dist_patterns:
        try:
            if not local_store.has_lone_file_matching(supp_dist_pattern):

                email_content = supplementary_distribution_not_found_email(
                    supp_dist_pattern
                )
                email_client.send(
                    submitter_email, email_content.subject, email_content.message
                )
                logger.info(
                    "Sent email to submitter: ",
                    submitter_email,
                    " about missing supplementary distribution: ",
                    supp_dist_pattern,
                )
                de_notifier.failure()
        except Exception as err:
            logger.error(
                f"Error while looking for supplementary distribution {supp_dist_pattern}",
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

    # Get the positional arguments (the inputs) from the pipeline_config
    # dict and run the specified sanity checker for it
    args = []
    for match, sanity_checker in pipeline_config["transform_inputs"].items():
        try:
            input_file_path: Path = local_store.save_lone_file_matching(match)
            logger.info(
                "Successfully saved file that matches pattern",
                data={
                    "input_file_path": input_file_path,
                    "match": match,
                    "files_in_directory": files_in_directory,
                },
            )
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            logger.error(
                "Error occurred while attempting to save matching pattern file.",
                err,
                data={
                    "match": match,
                    "pipeline_config": pipeline_config,
                    "files_in_directory": files_in_directory,
                },
            )

            de_notifier.failure()
            raise err

        try:
            sanity_checker(input_file_path)
            logger.info(
                "Successfully ran sanity check on input file path.",
                data={"input_file_path": input_file_path},
            )
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            logger.error(
                "Error occurred while running sanity checker on input file path.",
                err,
                data={
                    "input_file_path": input_file_path,
                    "pipeline_config": pipeline_config,
                    "files_in_directory": files_in_directory,
                },
            )

            de_notifier.failure()
            raise err

        args.append(input_file_path)

    # Get the transform function and keyword arguments from the transform_details
    transform_function = pipeline_config["transform"]
    kwargs = pipeline_config["transform_kwargs"]
    logger.info(
        "Retrieved transform function and keyword arguments from transform details.",
        data={
            "pipeline_config": pipeline_config,
            "args_got": args,
            "kwargs_got": kwargs,
        },
    )

    try:
        csv_path, metadata_path = transform_function(*args, **kwargs)
        logger.info(
            "Successfully ran transform function",
            data={
                "pipeline_config": pipeline_config,
                "csv_path": csv_path,
                "metadata_path": metadata_path,
            },
        )

    except Exception as err:
        logger.error(
            "Error occurred while running transform function",
            err,
            data={
                "transform_function": transform_function,
                "pipeline_config": pipeline_config,
                "args": args,
                "kwrags": kwargs,
            },
        )
        de_notifier.failure()
        raise err

    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    print("Worked. I ran to completion.")

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
        upload_client.upload_new_csv(csv_path, florence_access_token)
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
        logger.info("Got files", data={"files": all_files})
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
                    upload_client.upload_new_sdmx(supp_dist_path, florence_access_token)
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
