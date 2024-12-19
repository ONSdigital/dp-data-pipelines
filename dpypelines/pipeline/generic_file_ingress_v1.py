import os

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
)
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.utils import (
    get_email_client,
    get_mimetype,
    get_submitter_email,
)
from dpypelines.pipeline.utils import get_notifier, get_upload_client
from dpypelines.pipeline.validate_ingest_files import (
    file_size_0,
    metadata_json_is_parseable,
)

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
    de_notifier = get_notifier()

    # Create local data store from files directory
    try:
        local_store = LocalDirectoryStore(files_dir)
        files_in_directory = local_store.get_file_names()
        logger.info(
            "Local data store created",
            data={
                "local_store": local_store,
                "files_dir": files_dir,
                "files_in_directory": files_in_directory,
            },
        )
    except Exception as err:
        logger.error(
            "Failed to create local directory store from files directory",
            err,
            data={"files_dir": files_dir},
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
    for file in files_in_directory:
        try:
            if not local_store.has_lone_file_matching(file):
                try:
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
            upload_url = os.environ.get("UPLOAD_SERVICE_URL")
            assert upload_url, "UPLOAD_SERVICE_URL environment variable not set"
            logger.info("Retrieved Upload Service URL", data={"upload_url": upload_url})
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

    if not skip_data_upload:
        # Upload output files to Upload Service
        upload_client = get_upload_client(upload_url)
        for required_file in required_file_patterns:
            try:
                required_file_path = local_store.get_pathlike_of_file_matching(
                    required_file
                )
                logger.info(
                    "File to be uploaded retrieved",
                    data={"file_path": required_file_path},
                )
            except Exception as err:
                logger.error(
                    "Failed to retrieve file to be uploaded",
                    err,
                    data={"file_name": required_file},
                )
                de_notifier.failure()
                raise err

            try:
                mimetype = get_mimetype(required_file_path.suffix)
                if mimetype:
                    upload_client.upload_new(required_file_path, mimetype)
                else:
                    raise NotImplementedError(
                        f"Uploading file type {required_file_path.suffix} not currently supported."
                    )
                logger.info(
                    "File uploaded",
                    data={"file_path": required_file_path},
                )
                email_content = successful_file_upload_email(required_file_path.name)
                email_client.send(
                    submitter_email, email_content.subject, email_content.message
                )
            except Exception as err:
                logger.error(
                    "Failed to upload file",
                    err,
                    data={"file_path": required_file_path},
                )
                de_notifier.failure()
                email_content = failed_file_upload_email(required_file, str(err))
                email_client.send(
                    submitter_email, email_content.subject, email_content.message
                )
                raise err

    email_content = submission_processed_email()
    email_client.send(submitter_email, email_content.subject, email_content.message)
    de_notifier.success()
