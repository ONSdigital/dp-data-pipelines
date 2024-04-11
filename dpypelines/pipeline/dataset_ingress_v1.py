import json
from pathlib import Path

from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared.config import get_transform_identifier_from_config
from dpypelines.pipeline.shared.details import all_transform_details
from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.pipelineconfig import matching


def dataset_ingress_v1(files_dir: str):
    """
    Version one of the dataset ingress pipeline.

    files_dir: Path to the directory where the input
    files for this pipeline are located.

    Args:
        files_dir (str): Path to the directory where the input files for this pipeline are located.

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
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(f"Failed to access local data at {files_dir}", err)
        ) from err

    # Load the pipeline configuration as a dictionary
    try:
        pipeline_config: dict = local_store.get_lone_matching_json_as_dict(
            r"^pipeline-config.json$"
        )
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error("Failed to get pipeline-config.json", err)
        ) from err

    # Check for the existence of a pipeline configuration file
    try:
        if not local_store.has_lone_file_matching(r"^pipeline-config.json$"):
            de_notifier.failure()
            msg = message.expected_local_file_missing(
                "Pipeline config not found",
                "pipeline-config.json",
                "dataset_ingress_v1",
                local_store,
                local_store,
            )
            raise ValueError(msg)
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(
                "Error while checking for pipeline-config.json", err
            )
        ) from err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error("Failed to get required files patterns", err)
        ) from err

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
            de_notifier.failure()
            raise Exception(
                message.unexpected_error(
                    f"Error while looking for required file {required_file}", err
                )
            ) from err

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supplementary_distribution_patterns = (
            matching.get_supplementary_distribution_patterns(pipeline_config)
        )
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(
                "Failed to get supplementary distribution patterns", err
            )
        ) from err

    # Check for the existence of each supplementary distribution
    for supplementary_distribution in supplementary_distribution_patterns:
        try:
            if not local_store.has_lone_file_matching(supplementary_distribution):
                de_notifier.failure()
                msg = message.expected_local_file_missing(
                    f"Supplementary distribution {supplementary_distribution} not found",
                    supplementary_distribution,
                    "dataset_ingress_v1",
                    local_store,
                )
                raise ValueError(msg)
        except Exception as err:
            de_notifier.failure()
            raise Exception(
                message.unexpected_error(
                    f"Error while looking for supplementary distribution {supplementary_distribution}",
                    err,
                )
            ) from err

    # Get transform identifier from the config
    try:
        transform_identifier = get_transform_identifier_from_config(pipeline_config)
    except Exception as err:
        de_notifier.failure()
        raise Exception(
            message.unexpected_error(
                f"""
            Failed to get transform identifier from transform details", 
            {json.dumps(all_transform_details, indent=2, default=lambda x: str(x))}
            """,
                err,
            )
        )
        raise err

    # Use the identifier to get the transform details
    if transform_identifier not in all_transform_details.keys():
        de_notifier.failure()
        msg = message.unknown_transform(transform_identifier, all_transform_details)
        raise ValueError(msg)
    transform_details: dict = all_transform_details[transform_identifier]

    # Get the positional arguments (the inputs) from the transform_details
    # dict and run the specified sanity checker for it
    args = []
    for match, sanity_checker in transform_details["transform_inputs"].items():
        try:
            input_file_path: Path = local_store.save_lone_file_matching(match)
        except Exception as err:
            de_notifier.failure()
            printable_transform_details = json.dumps(
                transform_details, indent=2, default=lambda x: str(x)
            )
            raise Exception(
                message.pipeline_input_exception(
                    printable_transform_details, local_store, err
                )
            ) from err

        try:
            sanity_checker(input_file_path)
        except Exception as err:
            de_notifier.failure()
            raise Exception(
                message.pipeline_input_sanity_check_exception(
                    transform_details, local_store, err
                )
            ) from err

        args.append(input_file_path)

    # Get the transform function and keyword arguments from the transform_details
    transform_function = transform_details["transform"]
    kwargs = transform_details["transform_kwargs"]

    try:
        csv_path, metadata_path = transform_function(*args, **kwargs)
    except Exception as err:
        de_notifier.failure()
        printable_transform_details = json.dumps(
            transform_details, indent=2, default=lambda x: str(x)
        )
        raise Exception(
            message.error_in_transform(printable_transform_details, local_store, err)
        ) from err

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
