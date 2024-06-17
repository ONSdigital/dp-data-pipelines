import os

from dpytools.http.upload import UploadServiceClient
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_template_message import file_not_found_email
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig.matching import (
    get_required_files_patterns,
)
from dpypelines.pipeline.shared.utils import get_email_client, get_submitter_email

logger = DpLogger("data-ingress-pipelines")


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
                raise FileNotFoundError(
                    f"Could not find file matching pattern {required_file}"
                )
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
            for required_file in required_file_patterns:
                if "manifest.json" in required_file:
                    continue
                elif ".xml" in required_file:
                    upload_client.upload_new_sdmx(files_dir)
                    logger.info(
                        "UploadClient created from upload_url",
                        data={"upload_url": upload_url},
                    )
                elif ".csv" in required_file:
                    upload_client.upload_new_csv(files_dir)

                    logger.info(
                        "UploadClient created from upload_url",
                        data={"upload_url": upload_url},
                    )
                else:
                    raise TypeError(
                        f"File type of {required_file} currently not supported "
                    )
        except Exception as err:
            logger.error(
                "Error creating UploadClient", err, data={"upload_url": upload_url}
            )
            de_notifier.failure()
            raise err

    de_notifier.success()
