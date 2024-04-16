import os
import json
import os
import re
from pathlib import Path

from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.email.ses.client import SesClient
from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig import matching


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
        files_in_directory = local_store.get_file_names()
    except Exception as err:
        de_notifier.failure()
        raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        de_notifier.failure()
        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                de_notifier.failure()
                msg = message.expected_local_file_missing(
                    f"Required file {required_file} not found",
                    required_file,
                    "dataset_ingress_v1",
                    local_store,
                )
                raise ValueError(msg)
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            de_notifier.failure()
            raise err

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supp_dist_patterns = matching.get_supplementary_distribution_patterns(
            pipeline_config
        )
    except Exception as err:
        files_in_directory = local_store.get_file_names()
        de_notifier.failure()
        raise err

    # Check for the existence of each supplementary distribution
    for supp_dist_pattern in supp_dist_patterns:
        try:
            if not local_store.has_lone_file_matching(supp_dist_pattern):
                raise FileNotFoundError(
                    f"Could not find file matching pattern {supp_dist_pattern}"
                )
        except Exception as err:
            de_notifier.failure()
            raise err

    # Get the positional arguments (the inputs) from the pipeline_config
    # dict and run the specified sanity checker for it
    args = []
    for match, sanity_checker in pipeline_config["transform_inputs"].items():
        try:
            input_file_path: Path = local_store.save_lone_file_matching(match)
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            de_notifier.failure()
            raise err

        try:
            sanity_checker(input_file_path)
        except Exception as err:
            files_in_directory = local_store.get_file_names()
            de_notifier.failure()
            raise err

        args.append(input_file_path)

    # Get the transform function and keyword arguments from the transform_details
    transform_function = pipeline_config["transform"]
    kwargs = pipeline_config["transform_kwargs"]

    try:
        csv_path, metadata_path = transform_function(*args, **kwargs)

    except Exception as err:
        de_notifier.failure()
        printable_transform_details = json.dumps(
            pipeline_config, indent=2, default=lambda x: str(x)
        )
        raise Exception(
            message.error_in_transform(printable_transform_details, local_store, err)
        ) from err

    de_notifier.success()
    print("Worked. I ran to completion.")
