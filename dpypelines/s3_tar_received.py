import os
import json

from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json.validation import validate_json_schema

from dpypelines.pipeline.shared import notification as notify
from dpypelines.pipeline.shared.schemas import get_config_schema_path
from dpypelines.pipeline.shared import message

def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Example s3_object_name: my-bucket/my-data.tar
    """

    # Decompress the tar to ./inputs
    try:
        decompress_s3_tar(s3_object_name, "inputs")
        notify.data_engineering(f"Received s3 submission: s3://{s3_object_name}")
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error(f"Unable to decompress s3 object: {s3_object_name}", err)
            )
        raise err

    # Create a local directory store using our new files
    try:
        store = LocalDirectoryStore(f"inputs")
        notify.data_engineering(f'LocalDirectoryStore created using: {store.get_current_source_pathlike()}')
        notify.data_engineering(f'LocalDirectoryStore contains files: {store.get_file_names()}')
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error(f'Unable to create directory store from: ./"inputs"', err)
        )
        raise err

    # Load the pipeline configuration as a dictionary
    try:
        pipeline_config: dict = store.get_lone_matching_json_as_dict("pipeline-config.json")
        notify.data_engineering(f'Got pipeline config of {json.dumps(pipeline_config, indent=2)}')
    except Exception as err:
        notify.data_engineering(message.unexpected_error("Failed to get pipeline config", err))
        raise err

    # Use the schema for the config
    try:
        path_to_config_scheam = get_config_schema_path(pipeline_config)
        notify.data_engineering(f"Got config schema as {path_to_config_scheam}")
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error(f"Unable to get config schema from {json.dumps(pipeline_config, indent=2)}", err)
            )
        raise err
    
