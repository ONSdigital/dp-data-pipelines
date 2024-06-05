import os
import re

from dpytools.http.upload import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.shared.email_template_message import (
    file_not_found_email,
    submission_processed_email,
    supplementary_distribution_not_found_email,
)
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig.matching import (
    get_required_files_patterns,
    get_supplementary_distribution_patterns,
)
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_submitter_email,
    str_to_bool,
)

logger = DpLogger("generic-ingress-pipeline-v1")


def generic_file_ingress_v1(files_dir: str, pipeline_config: dict):
    """
    Version 1 of the generic file ingress pipeline.

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
        submitter_email = get_submitter_email()
        logger.info(
            "Got submitter email",
            data={"submitter_email": submitter_email},
        )
    except Exception as err:
        logger.error("Error occurred when getting submitter email", err)
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

    # just throw out an email to see if it works
    try:
        email_content = submission_processed_email()
        email_client.send(submitter_email, email_content.subject, email_content.message)
        logger.info(
            "Email sent",
            data={"submitter_email": submitter_email, "email_content": email_content},
        )
    except Exception as err:
        logger.error("Error occurred when sending email", err)
        de_notifier.failure()
        raise err

    # Get Upload Service URL from environment variable
    try:
        upload_url = os.environ.get("UPLOAD_SERVICE_URL", None)
        assert (
            upload_url is not None
        ), "UPLOAD_SERVICE_URL environment variable not set."
        logger.info("Got Upload Service URL", data={"upload_url": upload_url})
    except Exception as err:
        logger.error("Error occurred when getting Upload Service URL", err)
        de_notifier.failure()
        raise err

    # Attempt to access the local data store
    try:
        local_store = LocalDirectoryStore(files_dir)
        files_in_directory = local_store.get_file_names()
        logger.info(
            "Local data store created",
            data={
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

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = get_required_files_patterns(pipeline_config)
        logger.info(
            "Required file patterns retrieved from pipeline config",
            data={"required_file_patterns": required_file_patterns},
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Error occurred when getting required file patterns",
            err,
            data={
                "files_in_directory": files_in_directory,
                "pipeline_config": pipeline_config,
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
                    "Email sent to submitter about missing required file",
                    data={
                        "submitter_email": submitter_email,
                        "required_file": required_file,
                    },
                )
                de_notifier.failure()
        except Exception as err:
            files_in_directory = local_store.get_file_names()
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
        supp_dist_patterns = get_supplementary_distribution_patterns(pipeline_config)
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
                    logger.error(
                        "Email sent to submitter about missing supplementary distribution file",
                        err,
                        data={
                            "submitter_email": submitter_email,
                            "supplementary_distribution_pattern": supp_dist_pattern,
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
