
"""
Functions to create nicely formatted and informative messages.
"""
from pathlib import Path

def unexpected_error(msg: str, error: Exception) -> str:
    """
    We've caught an unexpected error. Make a sensible message explaining
    the problem.
    
    TODOL Literally a function that given a message and a python exception
    makes some sort of human readable output.
    """
    human_readable_output = "An unexpected error occured with error message: {msg}"

    return human_readable_output


def cant_find_scheama(config_dict, err: Exception) -> str:
    """
    We got an error when trying to identify the schema for the pipeline-conifg.json using the
    pipeline-config.json.

    Using the config-dict and the exceptioncreate a clear and informative message about what
    the problem is.
    
    TODO: Pretty much create a human readable message str that includes the config
    dict you were given and the exception that was raised upon trying to use it.
    """
    message = "We got an error when trying to identify the schema for the pipeline-conifg.json using the pipeline-config.json: {config_dict}"
    return message


def invlaid_config(config_dict, error: Exception) -> str:
    """
    The pipeline config that was provided is failing to validate.

    Provide suitable information so someone can find out why.
    
    TODO: Again, just combine the config dictionary and an exception
    into something readable.
    """
    message = "The pipeline config that was provided is failing to validate: {config_dict}"
    return message


def unknown_pipeline(pipeline_name: str, pipeline_options: dict) -> str:
    """
    We've been given a pipeline identifier that we don't recnognise. Use the name of
    the runing pipeline and the pipeline configuration to create a meaningful message
    explaining the problem.
    
    TODO:
    Look at the all_pipeline_details dict about half way down this example:
    https://github.com/GSS-Cogs/idpd-pipeline-sketch/blob/main/pipeline.ipynb
    - the pipeline_name is the key in the dict
    - the pipeline_options is the dict.
    
    Explain the key is not in the dict in a really hand holding user friendy way.
    """
    message = """
    Pipeline name is missing from the pipeline configurations.
    
    Pipeline: {pipeline_name}
    Pipeline Configurations: {pipeline_options}
    """
    return message


def metadata_validation_error(metadata_path, error: Exception) -> str:
    """
    The metadata has generated as validation error. Use the metadata and the error to create a
    sensible message explaining the problem.
    
    TODO: given  a path to some json metadata thats not valid and the exception
    that was raised trying to validate it, make a user friendly message.
    """
    message = "Metadata json file has failed validation: {metadata_path}"
    return message


def expected_local_file_missing(msg: str, file_path: Path, pipeline_name: str) -> str:
    """
    We're looking for a file on the local machine/runner and cannot find it.

    Create a sensible message.

    TODO: Something like:
    
    "pipeline {pipeline_name} encountered an issue finding a local file: {msg}"
    
    Or similar, make it easy to undersatand.    
    """
    message = """
    A pipeline has encountered an issue finding a local file.
    
    Pipeline's name: {pipeline_name}
    File: {file_path}
    Error message: {msg}
    """
    return message