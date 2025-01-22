import logging
from pathlib import Path

from dpypelines.pipeline.shared.pipelineconfig.transform import get_transform_details

logger = logging.getLogger("transform_processing")

"""
This can be archived - have had confirmation from DEs that no SDMX files are coming through
tracker so we will pull this back in when the requirement is there.

This part of the code will be revisited in the future.
"""


def process_transform(local_store, pipeline_config, files_in_directory, de_notifier):
    """
    Handles the transform process for input files and pipeline configurations.

    Args:
        local_store: The local data store containing files.
        pipeline_config: Dictionary containing pipeline configuration details.
        files_in_directory: List of files in the directory.
        de_notifier: Notification object for status updates.

    Returns:
        tuple: csv_path and metadata_path from the transform function.

    Raises:
        Exception: If any error occurs during the process.
    """
    # Get the transform inputs from the pipeline_config and run the specified sanity checker for it
    input_file_paths = []
    transform_inputs = get_transform_details(pipeline_config, "transform_inputs")

    for pattern, sanity_checker in transform_inputs.items():
        try:
            input_file_path: Path = local_store.get_pathlike_of_file_matching(pattern)
            logger.info(
                "Retrieved input file that matches pattern",
                extra={
                    "input_file_path": input_file_path,
                    "pattern": pattern,
                    "files_in_directory": files_in_directory,
                },
            )
        except Exception as err:
            logger.error(
                "Failed to retrieve input file matching pattern",
                exc_info=err,
                extra={
                    "pattern": pattern,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )
            de_notifier.failure()
            raise err

        try:
            sanity_checker(input_file_path)
            logger.info(
                "Sanity check run on input file path.",
                extra={
                    "sanity_checker": sanity_checker,
                    "input_file_path": input_file_path,
                },
            )
        except Exception as err:
            logger.error(
                "Error occurred when running sanity checker on input file path.",
                exc_info=err,
                extra={
                    "input_file_path": input_file_path,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )
            de_notifier.failure()
            raise err

        input_file_paths.append(input_file_path)

    # Get the transform function and arguments from pipeline config
    transform_function = get_transform_details(pipeline_config, "transform")
    transform_kwargs = get_transform_details(pipeline_config, "transform_kwargs")

    logger.info(
        "Retrieved transform function and transform_kwargs from pipeline config",
        extra={
            "transform_function": transform_function,
            "transform_kwargs": transform_kwargs,
            "input_file_paths": input_file_paths,
        },
    )

    try:
        csv_path, metadata_path = transform_function(
            *input_file_paths, **transform_kwargs
        )
        logger.info(
            "Transform function executed successfully",
            extra={
                "transform_function": transform_function,
                "input_file_paths": input_file_paths,
                "transform_kwargs": transform_kwargs,
                "csv_path": csv_path,
                "metadata_path": metadata_path,
            },
        )
        return csv_path, metadata_path

    except Exception as err:
        logger.error(
            "Transform function execution failed",
            exc_info=err,
            extra={
                "transform_function": transform_function,
                "input_file_paths": input_file_paths,
                "transform_kwargs": transform_kwargs,
                "pipeline_config": pipeline_config,
            },
        )
        de_notifier.failure()
        raise err
