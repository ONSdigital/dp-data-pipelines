from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.configuration import get_dataset_id, get_pipeline_config
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
        logger.info(
            "de_notifier successfully instantiated", data={"type": type(de_notifier)}
        )
    except Exception as err:
        logger.error("Failed to instanite de_notifier.", err)
        raise err

    # Decompress the tar file to the workspace
    try:
        decompress_s3_tar(s3_object_name, "input")
        logger.info(
            "S3 tar recieved decompressed to ./input",
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
            "failed to create local directory store using decompresed files", err
        )
        de_notifier.failure()
        raise err

    # get_dataset_id() currently empty - returns "not-specified"
    # To be updated once we know where the dataset_id can be extracted from (not necessarily s3_object_name as suggested by argument name)
    try:
        dataset_id = get_dataset_id(s3_object_name)
        logger.info(
            "Successfully retrieved dataset_id", data={"dataset_id": dataset_id}
        )
    except Exception as err:
        logger.error("Failed to retrieve dataset_id", err)
        de_notifier.failure()
        raise err

    # Get config details for the given dataset_id
    try:
        pipeline_config, config_keys = get_pipeline_config(dataset_id)
        logger.info(
            "Successfully retrieved config details for given dataset_id",
            data={"pipeline_config": pipeline_config, "dataset_id": dataset_id},
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve config details for given dataset_id",
            err,
            data={"dataset_id": dataset_id},
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
