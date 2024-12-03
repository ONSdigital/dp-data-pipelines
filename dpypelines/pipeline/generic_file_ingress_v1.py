import os

from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.utilities.utilities import str_to_bool

from dpypelines.pipeline.shared.email_template_message import file_not_found_email
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.utils import get_email_client, get_submitter_email
from dpypelines.pipeline.utils import get_notifier, upload_file

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
    notifier = get_notifier()
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
            data={"local_store_dir": files_dir},
        )
        notifier.failure()
        raise err

    try:
        manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")
        logger.info(
            "Retrieved manifest.json", 
            data={"manifest_dict": manifest_dict},
        )
    except Exception as err:
        logger.error("Failed to retrieve manifest.json", err)
        notifier.failure()
        raise err

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
        notifier.failure()
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
        notifier.failure()
        raise err

    # Get Upload Service URL from environment variable
    skip_data_upload = os.environ.get("SKIP_DATA_UPLOAD", False)
    if skip_data_upload:
        try:
            skip_data_upload = str_to_bool(skip_data_upload)
        except Exception as err:
            logger.error(
                "Failed to cast SKIP_DATA_UPLOAD to boolean", 
                err,
                data={"value": skip_data_upload},
            )
            notifier.failure()
            raise err

    if not skip_data_upload:
        try:
            upload_url = os.environ.get("UPLOAD_SERVICE_URL")
            assert upload_url, "UPLOAD_SERVICE_URL environment variable not set"
            logger.info("Retrieved Upload Service URL", data={"upload_url": upload_url})
        except Exception as err:
            logger.error("Failed to retrieve Upload Service URL", err)
            notifier.failure()
            raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = get_matching_pattern(pipeline_config, "required_files")
        logger.info(
            "Retrieved required file patterns", 
            data={
                "required_file_patterns": required_file_patterns,
                "pipeline_config": pipeline_config,
            },
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve required file patterns", 
            err,
            data={
                "pipeline_config": pipeline_config,
            },
        )
        notifier.failure()
        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                try:
                    raise FileNotFoundError(
                        f"No file found matching pattern {required_file}"
                    )
                except FileNotFoundError as err:
                    email_content = file_not_found_email(required_file)
                    email_client.send(
                        submitter_email, email_content.subject, email_content.message
                    )
                    logger.error(
                        "Required file not found", 
                        err, 
                        data={"required_file": required_file},
                    )
                    notifier.failure()
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
            notifier.failure()
            raise err

    # Allow DE's to skip the upload to s3 part of the pipeline while
    # developing code locally.
    logger.info(
        "skip_data_upload set from SKIP_DATA_UPLOAD env var",
        data={"value": skip_data_upload},
    )
    if not skip_data_upload:
        # Upload output files to Upload Service
        upload_client = upload_file(upload_url)
        for required_file in required_file_patterns:
            try:
                required_file_path = local_store.get_pathlike_of_file_matching(required_file)
                logger.info("File to be uploaded retrieved", data={"file_path": required_file_path})
            except Exception as err:
                logger.error(
                    "Failed to retrieve file to be uploaded", 
                    err, 
                    data={"file_name": required_file},
                )
                notifier.failure()
                raise err

            try:
                if required_file_path.suffix == ".csv":
                    upload_client.upload_new_csv(required_file_path)
                elif required_file_path.suffix == ".xml":
                    upload_client.upload_new_sdmx(required_file_path)
                elif required_file_path.suffix == ".json":
                    upload_client.upload_new_json(required_file_path)
                else:
                    raise NotImplementedError(f"Uploading file type {required_file_path.suffix} not currently supported.")
                logger.info(
                    "File uploaded", 
                    data={"file_path": required_file_path},
                )
            except Exception as err:
                logger.error(
                    "Failed to upload file", 
                    err, 
                    data={"file_path": required_file_path},
                )
                notifier.failure()
                raise err

    notifier.success()
