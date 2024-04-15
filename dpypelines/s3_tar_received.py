from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.configuration import get_dataset_id, get_pipeline_config
from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)


def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Args:
        s3_object_name (str): The S3 object name of the tar file to be processed.
        Example: 'my-bucket/my-data.tar'

    """
    # Create notifier from webhook env var
    de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
        "DE_SLACK_WEBHOOK"
    )

    # Decompress the tar file to the workspace
    try:
        decompress_s3_tar(s3_object_name, "input")
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(
                f"Failed to decompress tar file {s3_object_name}", err
            )
        ) from err

    # Create a local directory store using the decompressed files
    try:
        local_store = LocalDirectoryStore("input")
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(
                "Failed to create local directory store at inputs", err
            )
        ) from err

    # get_dataset_id() currently empty - returns "not-specified"
    # To be updated once we know where the dataset_id can be extracted from (not necessarily s3_object_name as suggested by argument name)
    try:
        dataset_id = get_dataset_id(s3_object_name)
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(f"Failed to get dataset id {dataset_id}", err)
        ) from err

    # Get config details for the given dataset_id
    try:
        pipeline_config, config_keys = get_pipeline_config(dataset_id)
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(
                f"Failed to match dataset_id {dataset_id} to available configuration keys {config_keys}",
                err,
            )
        ) from err

    # Get the path to the directory
    files_dir = local_store.get_current_source_pathlike()

    # Call the secondary_function specified in pipeline_config
    secondary_function = pipeline_config["secondary_function"]
    secondary_function(files_dir, pipeline_config)
