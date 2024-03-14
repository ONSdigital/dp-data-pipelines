import importlib
import json
import os

from dpytools.stores.directory.local import LocalDirectoryStore
from dpypelines.pipeline.shared import notification as notify
from dpypelines.pipeline.shared.schemas import get_config_schema_path
from dpytools.s3.basic import decompress_s3_tar
from dpytools.validation.json import validation
from dpypelines.pipeline.shared import message

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
        notify.data_engineering(message.unexpected_error(f"Failed to decompress tar file {s3_object_name}", err))
        raise err
    
    # Identify the child directory that was just decompressed within /inputs
    try:
        directories = [d for d in os.listdir("inputs") if os.path.isdir(os.path.join("inputs", d))]
        assert len(directories) == 1, (
            "Aborting, input directory has more than one directory within it"
        )
        decompressed_directory = directories[0]
    except Exception as err:
        notify.data_engineering(message.unexpected_error(f"Failed to identify decompressed directory in 'inputs'. Found directories: {directories}", err))
        raise err

    # Create a local directory store using the decompressed files
    try:
        localStore = LocalDirectoryStore(f"inputs/{decompressed_directory}")
    except Exception as err:
        notify.data_engineering(message.unexpected_error(f"Failed to create local directory store at inputs/{decompressed_directory}", err))
        raise err

    # Verify if a pipeline configuration file ('pipeline-config.json') exists in the local directory store
    try:
        # Check for the existence of a configuration file
        if not localStore.has_lone_file_matching("pipeline-config.json"):
            msg = message.expected_local_file_missing("Missing pipeline-config.json", "pipeline-config.json", "start")
            notify.data_engineering(msg)
            # Create a default configuration file if one does not exist
            default_config = {
                "pipeline": "default_pipeline",
                "parameters": {}
            }
            with open("pipeline-config.json", "w") as f:
                json.dump(default_config, f)
        else:
            # Load the configuration file
            config_dict = localStore.get_lone_matching_json_as_dict("pipeline-config.json")     
    except Exception as err:
        notify.data_engineering(message.unexpected_error("Error while checking for pipeline-config.json", err))
        raise err

    # Load the configuration file and validate it against a schema
    try:
        config_dict = localStore.get_lone_matching_json_as_dict("pipeline-config.json")
    except Exception as err:
        notify.data_engineering(message.pipeline_input_exception({"pipeline-config.json": "expected"}, localStore, err))
        raise err
    
    # Retrieve the path to the schema for the configuration
    try:
        path_to_schema = get_config_schema_path(config_dict)
    except Exception as err:
        notify.data_engineering(message.cant_find_schema(config_dict, err))
        raise err

    # Validate the configuration against the retrieved schema
    try:    
        validation.validate_json_schema(schema_path=path_to_schema, data_dict=config_dict)
    except Exception as err:
        notify.data_engineering(message.invalid_config(config_dict, err))
        raise err

    # Loads the pipeline specified in the configuration file and executes it with the provided parameters
    try:
        # Load the pipeline specified in the configuration
        pipeline_name = config_dict.get("pipeline")
        if not pipeline_name:
            raise ValueError("Missing 'pipeline' field in configuration")

        # Import the pipeline module
        pipeline_module = importlib.import_module(f"pipeline.{pipeline_name}")

        # Run the pipeline
        pipeline_module.run(config_dict.get("parameters", {}))
    except Exception as err:
        notify.data_engineering(message.unexpected_error(f"Error while running pipeline {pipeline_name}", err))
        raise err