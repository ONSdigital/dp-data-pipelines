import os

from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json import validation


from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared import notification
from dpypelines.pipeline.shared.schemas import get_config_schema_path

de_messenger = notification.PipelineMessenger(os.environ.get("DE_SLACK_WEBHOOK", None))


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
        #raise Exception("force an exception for testing")
        #raise ValueError("Force an error to appear")
        decompress_s3_tar(s3_object_name, "input")
    except Exception as err:
        de_messenger.failure()
        raise ValueError("Force an error to appear")
        #raise Exception("Failed to decompress tar file",
         #   message.unexpected_error(
          #      f"Failed to decompress tar file {s3_object_name}", err
           # )
        #) from err

    # Create a local directory store using the decompressed files
    try:
        local_store = LocalDirectoryStore("input")
    except Exception as err:
        de_messenger.failure()
        raise Exception(
            message.unexpected_error(
                "Failed to create local directory store at inputs", err
            )
        ) from err

    # Check for the existence of a configuration file
    try:
        if not local_store.has_lone_file_matching(r"^pipeline-config.json$"):
            de_messenger.failure()
            msg = message.expected_local_file_missing(
                "Pipeline config not found",
                "pipeline-config.json",
                "dataset_ingress_v1",
                local_store,
            )
            raise ValueError(msg)
    except Exception as err:
        de_messenger.failure()
        raise Exception(
            message.unexpected_error(
                "Error while checking for pipeline-config.json", err
            )
        ) from err

    # Load the configuration file and validate it against a schema
    try:
        config_dict = local_store.get_lone_matching_json_as_dict(
            r"^pipeline-config.json$"
        )
    except Exception as err:
        de_messenger.failure()
        raise Exception(
            message.unexpected_error("Error while getting pipeline-config.json", err)
        ) from err

    # Retrieve the path to the schema for the configuration
    try:
        path_to_schema = get_config_schema_path(config_dict)
    except Exception as err:
        de_messenger.failure()
        raise Exception(message.cant_find_schema(config_dict, err)) from err

    # Validate the configuration against the retrieved schema
    try:
        validation.validate_json_schema(
            schema_path=path_to_schema,
            data_dict=config_dict,
            error_msg="Validating pipeline-config.json",
            indent=2,
        )
    except Exception as err:
        de_messenger.failure()
        raise Exception(message.invalid_config(config_dict, err)) from err

    # Get the path to the directory
    files_dir = local_store.get_current_source_pathlike()
    # Call the dataset_ingress_v1 function with the directory path
    dataset_ingress_v1(files_dir)
