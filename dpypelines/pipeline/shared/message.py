"""
Functions to create nicely formatted and informative messages.
"""

import json
from os import pipe
from pathlib import Path
from typing import Dict

from dpytools.stores.directory.base import BaseWritableSingleDirectoryStore


def unexpected_error(msg: str, error: Exception) -> str:
    """
    We've caught an unexpected error. Make a sensible message explaining
    the problem.
    """
    error_type = error.__class__.__name__
    message = f"""
        {msg}

        Error type: {error_type}
        Error: {error}
    """

    return message


def cant_find_schema(config_dict, error: Exception) -> str:
    """
    We got an error when trying to identify the schema for the pipeline-conifg.json using the
    pipeline-config.json.

    Using the config-dict and the exceptioncreate a clear and informative message about what
    the problem is."""
    formatted_config_dict = json.dumps(config_dict, indent=2)
    error_type = error.__class__.__name__
    message = f"""  
        We got an error when trying to identify the schema for the pipeline-config.json using the pipeline-config.json.
        
        Pipeline-config.json: {formatted_config_dict}
        
        Error type: {error_type}
        Error: {error}
    """
    return message


def invalid_config(config_dict, error: Exception) -> str:
    """
    The pipeline config that was provided is failing to validate.

    Provide suitable information so someone can find out why.
    """
    formatted_config_dict = json.dumps(config_dict, indent=2)
    error_type = error.__class__.__name__
    message = f"""
        The pipeline config that was provided is failing to validate.
        
        Pipeline-config.json: {formatted_config_dict}

        Error type: {error_type}
        Error: {error}
    """
    return message


def unknown_transform(transform_identifier: str, all_transform_details: dict) -> str:
    """
    We've been given a transform identifier that we don't recnognise. Create a
    meaningful message explaining the problem.
    """
    formatted_transform_options = json.dumps(all_transform_details, indent=2, default=lambda x: str(x))
    message = f"""
        Pipeline name is missing from the pipeline transform configurations.
        
        Pipeline: {transform_identifier}
        Pipeline Configurations: {formatted_transform_options}
    """
    return message


def metadata_validation_error(metadata_path, error: Exception) -> str:
    """
    The metadata has generated as validation error. Use the metadata and the error to create a
    sensible message explaining the problem.
    """
    error_type = error.__class__.__name__
    message = f"""
        Metadata json file has failed validation: {metadata_path}
        Error type: {error_type}
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


def pipeline_input_exception(
    pipeline_dict: Dict, store: BaseWritableSingleDirectoryStore, error: Exception
):
    """
    Some pipeline details specifiy a file but there was an issue getting it out of the store.
    """
    error_type = error.__class__.__name__
    formatted_pipeline_dict = json.dumps(pipeline_dict, indent=2)
    message = f"""
        File specified in pipeline details could not be retrieved from store: {store.local_path}
        Error type: {error_type}
        Error: {error}
        Pipeline details dictionary: {formatted_pipeline_dict}
    """
    return message


def error_in_transform(
    pipeline_dict, store: BaseWritableSingleDirectoryStore, error: Exception
) -> str:
    """
    An exception was raised during the transform process.
    """
    error_type = error.__class__.__name__
    formatted_pipeline_dict = json.dumps(pipeline_dict, indent=2)
    message = f"""
        An error occured during the transformation process for pipeline input with store: {store.local_path}
        Error type: {error_type}
        Error: {error}
        Pipeline details dictionary: {formatted_pipeline_dict}
    """
    return message


def pipeline_input_sanity_check_exception(
    pipeline_dict, store: BaseWritableSingleDirectoryStore, error: Exception
) -> str:
    """
    An exception was raised during the sanity checking process.
    """
    error_type = error.__class__.__name__
    formatted_pipeline_dict = json.dumps(pipeline_dict, indent=2)
    message = f"""
        An error was raised while performing a sanity check on pipeline with given store: {store.local_path}
        Error type: {error_type}
        Error: {error}
        Pipeline details dictionary: {formatted_pipeline_dict}
    """
    return message
