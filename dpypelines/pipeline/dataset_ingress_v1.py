import json
from pathlib import Path

from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig import matching

logger = DpLogger('data-ingress-pipeline')

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
    de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
        "DE_SLACK_WEBHOOK"
    )

    # Attempt to access the local data store
    try:
        local_store = LocalDirectoryStore(files_dir)
        logger.info("Local data store successfully instansiated", data ={"local_store_dir": files_dir, "local_store": local_store})
    except Exception as err:
        logger.error(f"Failed to access local data at {files_dir}", err, data={"local_store_dir": files_dir})
        de_notifier.failure()

        raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
        logger.info("Required file patterns retrieved from pipeline configuration", data={"required_file_patterns": required_file_patterns})
    except Exception as err:
        logger.error("Failed to get required files pattern", err, data={"pipeline_config": pipeline_config, "local_store": local_store})
        de_notifier.failure()

        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                logger.info(f"Required matching file {required_file} was not found.", data={"required_file": required_file})
                de_notifier.failure()
                msg = message.expected_local_file_missing(
                    f"Required file {required_file} not found",
                    required_file,
                    "dataset_ingress_v1",
                    local_store,
                )
                raise ValueError(msg)
        except Exception as err:
            logger.error(f"Error while looking for required file {required_file}", 
            err, 
            data={"required_file": required_file, 
            "required_file_patterns": required_file_patterns, 
            "local_store": local_store})
            de_notifier.failure()

            raise err

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supplementary_distribution_patterns = (
            matching.get_supplementary_distribution_patterns(pipeline_config)
        )
        logger.info("Successfully retrieved supplementary distribution patterns from pipeline config",
         data={"supplementary_distribution_pattenrs": supplementary_distribution_patterns})
    except Exception as err:
        logger.error("Failed to get supplementary distribution patterns", err, data={"local_store": local_store})
        de_notifier.failure()

        raise err

    # Check for the existence of each supplementary distribution
    for supplementary_distribution in supplementary_distribution_patterns:
        try:
            if not local_store.has_lone_file_matching(supplementary_distribution):
                logger.info(f"Supplementary distribution {supplementary_distribution} not found", data={"supplementary_distribution": supplementary_distribution})
                de_notifier.failure()
                msg = message.expected_local_file_missing(
                    f"Supplementary distribution {supplementary_distribution} not found",
                    supplementary_distribution,
                    "dataset_ingress_v1",
                    local_store,
                )
                raise ValueError(msg)
        except Exception as err:
            logger.error(f"Error while looking for supplementary distribution {supplementary_distribution}", 
            err, 
            data={"supplementary_distribution": supplementary_distribution}
            )

            de_notifier.failure()
            raise err

    # Get the positional arguments (the inputs) from the pipeline_config
    # dict and run the specified sanity checker for it
    args = []
    for match, sanity_checker in pipeline_config["transform_inputs"].items():
        try:
            input_file_path: Path = local_store.save_lone_file_matching(match)
            logger.info(f"Successfully saved file that matches pattern to path {input_file_path}", data={"input_file_path": input_file_path})
        except Exception as err:
            logger.error("Error occured while attempting to save matching pattern file.", 
            err, 
            data={"match": match, 
            "pipeline_config": pipeline_config, 
            "local_store": local_store})    

            de_notifier.failure()

            raise err

        try:
            sanity_checker(input_file_path)
            logger.info("Successfully ran sanity check on input file path.", data={"input_file_path": input_file_path})
        except Exception as err:
            logger.error("Error occured while running sanity checker on input file path.", 
            err, 
            data={"input_file_path": input_file_path, 
            "pipeline_config": pipeline_config, 
            "local_store": local_store})

            de_notifier.failure()

            raise err

        args.append(input_file_path)

    # Get the transform function and keyword arguments from the transform_details
    transform_function = pipeline_config["transform"]
    kwargs = pipeline_config["transform_kwargs"]
    logger.info("Retrieved transform function and keyword arguments from transform details.", data={"pipeline_config": pipeline_config})

    try:
        csv_path, metadata_path = transform_function(*args, **kwargs)
        logger.info("Successfully retrieved csv path and metadata path with keyword args from transform function", 
        data={"transform_function": transform_function, 
        "pipeline_config": pipeline_config})

    except Exception as err:
        logger.error("Error occured while getting transform function and keyword arguments from transform details", 
        err, 
        data={"transform_function": transform_function, 
        "pipeline_config": pipeline_config, 
        "local_store": local_store}
        )
        de_notifier.failure()

        raise err

    # TODO - validate the metadata once we have a schema for it.

    # TODO - validate the csv once we know what we're validating

    # ---------------------------------
    # TODO - delete me at a later point
    # just an everything is ok alarm for
    # now so we know the right things happen
    # ---------------------------------

    with open(metadata_path) as f:
        metadata = json.load(f)

    import pandas as pd

    de_notifier.success()
    de_notifier.msg_str(
        f"""

Tranform ran to completion.

Data Snippet:
```
{pd.read_csv(csv_path)[:5]}
```

Metadata:
```
{json.dumps(metadata, indent=2)}
```
        """
    )

    print("Worked. I ran to completion.")
