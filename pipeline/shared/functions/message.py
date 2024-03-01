
"""
Functions to create nicely formatted and informative messages.
"""
from pathlib import Path
import json

def unexpected_error(msg: str, error: Exception) -> str:
    """
    We've caught an unexpected error. Make a sensible message explaining
    the problem.
    """
    message = f"""
        {msg}

        Error: {error}
    """

    return message


def cant_find_scheama(config_dict, err: Exception) -> str:
    """
    We got an error when trying to identify the schema for the pipeline-conifg.json using the
    pipeline-config.json.

    Using the config-dict and the exceptioncreate a clear and informative message about what
    the problem is."""
    formatted_config_dict = json.dumps(config_dict, indent=2)
    message = f"""  
        We got an error when trying to identify the schema for the pipeline-conifg.json using the pipeline-config.json.
        
        Pipeline-config.json: {formatted_config_dict}
        
        Error: {err}
    """
    return message


def invlaid_config(config_dict, error: Exception) -> str:
    """
    The pipeline config that was provided is failing to validate.

    Provide suitable information so someone can find out why.
    """
    formatted_config_dict = json.dumps(config_dict, indent=2)
    message = f"""
        The pipeline config that was provided is failing to validate.
        
        Pipeline-config.json: {formatted_config_dict}

        Error: {error}
    """
    return message


def unknown_pipeline(pipeline_name: str, pipeline_options: dict) -> str:
    """
    We've been given a pipeline identifier that we don't recnognise. Use the name of
    the runing pipeline and the pipeline configuration to create a meaningful message
    explaining the problem.
    """
    formatted_pipeline_options = json.dumps(pipeline_options, indent=2)
    message = f"""
        Pipeline name is missing from the pipeline configurations.
        
        Pipeline: {pipeline_name}
        Pipeline Configurations: {formatted_pipeline_options}
    """
    return message


def metadata_validation_error(metadata_path, error: Exception) -> str:
    """
    The metadata has generated as validation error. Use the metadata and the error to create a
    sensible message explaining the problem.
    """
    message = f"""
        Metadata json file has failed validation: {metadata_path}
        Error: {error}
    """
    return message


def expected_local_file_missing(msg: str, file_path: Path, pipeline_name: str) -> str:
    """
    We're looking for a file on the local machine/runner and cannot find it.
    """
    message = f"""
        A pipeline has encountered an issue finding a local file.
        
        Pipeline's name: {pipeline_name}
        File: {file_path}
        Message: {msg}
    """
    return message