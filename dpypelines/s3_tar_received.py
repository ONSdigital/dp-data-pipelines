import re
from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json import validation

from dpypelines.pipeline.configuration import CONFIGURATION, get_dataset_id
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.schemas import get_config_schema_path

de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook("DE_SLACK_WEBHOOK")


def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Args:
        s3_object_name (str): The S3 object name of the tar file to be processed.
        Example: 'my-bucket/my-data.tar'

    """

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
    dataset_id = get_dataset_id(s3_object_name)

    # Get config details for the given dataset_id
    for key in CONFIGURATION.keys():
        if re.match(key, dataset_id):
            pipeline_config = CONFIGURATION[key]

    # Get the path to the directory
    files_dir = local_store.get_current_source_pathlike()

    # Call the secondary_function specified in pipeline_config
    secondary_function = pipeline_config["secondary_function"]

    # Replace once `dataset_ingress_v1` arguments are updated to include pipeline_config
    secondary_function(files_dir)
    # secondary_function(files_dir, pipeline_config)
