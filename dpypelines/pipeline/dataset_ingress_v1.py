from pathlib import Path

from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig import matching

logger = DpLogger("data-ingress-pipeline")


def dataset_ingress_v1(files_dir: str, pipeline_config: dict):
    """
    Version one of the dataset ingress pipeline.

    files_dir: Path to the directory where the input
    files for this pipeline are located.

    Args:
        files_dir (str): Path to the directory where the input files for this pipeline are located.
        pipeline_config (dict): Dictionary of configuration details required to run the pipeline (determined by dataset id)

    Raises:
        ValueError: If required files, supplementary distributions, or pipeline configuration are not found in the input directory.
        Exception: If any other unexpected error occurs.
    """
    # Create notifier from webhook env var
    try:
        de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK"
        )
        logger.info("Created notifier instance", data={"notifier": de_notifier})
    except Exception as err:
        logger.error("Error occured while attempting to create notifier instance", err)
        raise err

    # Attempt to access the local data store
    try:
        local_store = LocalDirectoryStore(files_dir)
        files_in_directory = local_store.get_file_names()
        logger.info(
            "Local data store successfully instansiated",
            data={
                "local_store_dir": files_dir,
                "files_in_directory": files_in_directory,
            },
        )
    except Exception as err:
        logger.error(
            f"Failed to access local data at {files_dir}",
            err,
            data={"local_store_dir": files_dir},
        )
        de_notifier.failure()
        raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
        logger.info(
            "Required file patterns retrieved from pipeline configuration",
            data={"required_file_patterns": required_file_patterns},
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Failed to get required files pattern",
            err,
            data={
                "pipeline_config": pipeline_config,
                "files_in_directory": files_in_directory,
            },
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                raise FileNotFoundError(
                    f"Could not find file matching pattern {required_file}"
                )
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            logger.error(
                f"Error while looking for required file {required_file}",
                err,
                data={
                    "required_file": required_file,
                    "required_file_patterns": required_file_patterns,
                    "files_in_directory": files_in_directory,
                },
            )
            de_notifier.failure()
            raise err

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supplementary_distribution_patterns = (
            matching.get_supplementary_distribution_patterns(pipeline_config)
        )
        logger.info(
            "Successfully retrieved supplementary distribution patterns from pipeline config",
            data={
                "supplementary_distribution_pattenrs": supplementary_distribution_patterns
            },
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        logger.error(
            "Failed to get supplementary distribution patterns",
            err,
            data={"pipeline_config": pipeline_config},
        )
        de_notifier.failure()
        raise err

    # Check for the existence of each supplementary distribution
    for supplementary_distribution in supplementary_distribution_patterns:
        try:
            if not local_store.has_lone_file_matching(supplementary_distribution):
                raise FileNotFoundError(
                    f"Could not find file matching pattern {supplementary_distribution}"
                )
        except Exception as err:
            logger.error(
                f"Error while looking for supplementary distribution {supplementary_distribution}",
                err,
                data={
                    "supplementary_distribution": supplementary_distribution,
                    "supplementary_distribution_patterns": supplementary_distribution_patterns,
                    "files_in_directory": files_in_directory,
                    "pipeline_config": pipeline_config,
                },
            )
            de_notifier.failure()
            raise err

    # Get the positional arguments (the inputs) from the pipeline_config
    # dict and run the specified sanity checker for it
    args = []
    for match, sanity_checker in pipeline_config["transform_inputs"].items():
        try:
            input_file_path: Path = local_store.save_lone_file_matching(match)
            logger.info(
                "Successfully saved file that matches pattern",
                data={
                    "input_file_path": input_file_path,
                    "match": match,
                    "files_in_directory": files_in_directory,
                },
            )
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            logger.error(
                "Error occured while attempting to save matching pattern file.",
                err,
                data={
                    "match": match,
                    "pipeline_config": pipeline_config,
                    "files_in_directory": files_in_directory,
                },
            )

            de_notifier.failure()
            raise err

        try:
            sanity_checker(input_file_path)
            logger.info(
                "Successfully ran sanity check on input file path.",
                data={"input_file_path": input_file_path},
            )
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            logger.error(
                "Error occured while running sanity checker on input file path.",
                err,
                data={
                    "input_file_path": input_file_path,
                    "pipeline_config": pipeline_config,
                    "files_in_directory": files_in_directory,
                },
            )

            de_notifier.failure()
            raise err

        args.append(input_file_path)

    # Get the transform function and keyword arguments from the transform_details
    transform_function = pipeline_config["transform"]
    kwargs = pipeline_config["transform_kwargs"]
    logger.info(
        "Retrieved transform function and keyword arguments from transform details.",
        data={
            "pipeline_config": pipeline_config,
            "args_got": args,
            "kwargs_got": kwargs,
        },
    )

    try:
        csv_path, metadata_path = transform_function(*args, **kwargs)
        logger.info(
            "Successfully ran transform function",
            data={
                "pipeline_config": pipeline_config,
                "csv_path": csv_path,
                "metadata_path": metadata_path,
            },
        )

    except Exception as err:
        logger.error(
            "Error occured while running transform function",
            err,
            data={
                "transform_function": transform_function,
                "pipeline_config": pipeline_config,
                "args": args,
                "kwrags": kwargs,
            },
        )
        de_notifier.failure()
        raise err

    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    de_notifier.success()
    print("Worked. I ran to completion.")
