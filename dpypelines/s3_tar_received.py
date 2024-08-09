from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.configuration import get_pipeline_config, get_source_id
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)

logger = DpLogger("data-ingress-pipeline")


def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Args:
        s3_object_name (str): The S3 object name of the tar file to be processed.
        Example: 'my-bucket/my-data.tar'

    """

    # Create notifier from webhook env var
    try:
        de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK"
        )
        logger.info("Slack notifier created", data={"notifier": type(de_notifier)})
    except Exception as err:
        logger.error("Failed to create Slack notifier", err)
        raise err

    # Decompress the tar file to the workspace
    try:
        decompress_s3_tar(s3_object_name, "input")
        logger.info(
            "S3 tar received decompressed to ./input",
            data={"s3_object_name": s3_object_name},
        )
    except Exception as err:
        logger.error(
            "Failed to decompress tar file",
            err,
            data={"s3_object_name": s3_object_name},
        )
        de_notifier.failure()
        raise err

    # Create a local directory store using the decompressed files
    try:
        local_store = LocalDirectoryStore("input")
        logger.info(
            "local directory store successfully set up using decompressed files",
            data={"local store": local_store.get_file_names()},
        )
    except Exception as err:
        logger.error(
            "failed to create local directory store using decompressed files", err
        )
        de_notifier.failure()
        raise err

    try:
        manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")
        logger.info(
            "Successfully retrieved manifest.json as dict",
            data={
                "manifest_dict": manifest_dict,
            },
        )
    except Exception as err:
        logger.error("Failed to retrieve file: manifest.json", err)
        de_notifier.failure()
        raise err

    try:
        source_id = get_source_id(manifest_dict)
        logger.info("Successfully retrieved source_id", data={"source_id": source_id})
    except Exception as err:
        logger.error("Failed to retrieve source_id", err)
        de_notifier.failure()
        raise err

    # Get config details for the given source_id
    try:
        pipeline_config = get_pipeline_config(source_id)
        logger.info(
            "Successfully retrieved config details for given source_id",
            data={"pipeline_config": pipeline_config, "source_id": source_id},
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve config details for given source_id",
            err,
            data={"source_id": source_id},
        )
        de_notifier.failure()
        raise err

    # Get the path to the directory
    files_dir = local_store.get_current_source_pathlike()

    # Call the secondary_function specified in pipeline_config
    try:
        secondary_function = pipeline_config["secondary_function"]
        secondary_function(files_dir, pipeline_config)
        logger.info(
            "Successfully executed secondary function specified in pipeline_config",
            data={
                "secondary_function": secondary_function,
                "pipeline_config": pipeline_config,
            },
        )
    except Exception as err:
        logger.error(
            "Failed to executed secondary function specified in pipeline_config",
            err,
            data={"pipeline_config": pipeline_config, "files_dir": files_dir},
        )
        de_notifier.failure()
        raise err
