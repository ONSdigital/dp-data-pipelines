import json

from dpytools.stores.directory.local import LocalDirectoryStore
from dpypelines.pipeline.shared import message
from dpypelines.pipeline.shared import notification as notify
from dpypelines.pipeline.shared.config import get_transform_identifier_from_config
from dpypelines.pipeline.shared.pipelineconfig import matching
from dpypelines.pipeline.shared.details import all_transform_details


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

    # Attempt to access the local data store
    try:
        local_store = LocalDirectoryStore(files_dir)
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error(f"Failed to access local data at {files_dir}", err)
        )
        raise err

    # Load the pipeline configuration as a dictionary
    try:
        pipeline_config: dict = local_store.get_lone_matching_json_as_dict(
            r"^pipeline-config.json$"
        )
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error("Failed to get pipeline-config.json", err)
        )
        raise err

    # Check for the existence of a pipeline configuration file
    try:
        if not local_store.has_lone_file_matching(r"^pipeline-config.json$"):
            msg = message.expected_local_file_missing(
                "Pipeline config not found",
                "pipeline-config.json",
                "dataset_ingress_v1",
            )
            notify.data_engineering(msg)
            raise ValueError(msg)
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error("Failed to check for pipeline config", err)
        )
        raise err

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error("Failed to get required files patterns", err)
        )
        raise err

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                msg = message.expected_local_file_missing(
                    f"Required file {required_file} not found",
                    required_file,
                    "dataset_ingress_v1",
                )
                notify.data_engineering(msg)
                raise ValueError(msg)
        except Exception as err:
            notify.data_engineering(
                message.unexpected_error(
                    f"Error while looking for required file {required_file}", err
                )
            )
            raise err

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supplementary_distribution_patterns = (
            matching.get_supplementary_distribution_patterns(pipeline_config)
        )
    except Exception as err:
        notify.data_engineering(
            message.unexpected_error(
                "Failed to get supplementary distribution patterns", err
            )
        )
        raise err

    # Check for the existence of each supplementary distribution
    for supplementary_distribution in supplementary_distribution_patterns:
        try:
            if not local_store.has_lone_file_matching(supplementary_distribution):
                msg = message.expected_local_file_missing(
                    f"Supplementary distribution {supplementary_distribution} not found",
                    supplementary_distribution,
                    "dataset_ingress_v1",
                )
                notify.data_engineering(msg)
                raise ValueError(msg)
        except Exception as err:
            notify.data_engineering(
                message.unexpected_error(
                    f"Error while looging for supplementary distribution {supplementary_distribution}",
                    err,
                )
            )
            raise err

    # Get transform identifier from the config
    try:
        transform_identifier = get_transform_identifier_from_config(pipeline_config)
    except Exception as err:
        notify.data_engineering(message.unexpected_error(
            f"""
            Failed to get tranform details.", 
            
            transform_identifier: {transform_identifier}
            transform_details" {json.dumps(all_transform_details, indent=2, default=lambda x: str(x))}
            """, err
        ))

    # Use the identifier to get the transform details  
    if transform_identifier not in all_transform_details.keys():
        msg = message.unknown_transform(
            transform_identifier, all_transform_details
        )
        notify.data_engineering(msg)
        raise ValueError(msg)
    transform_details = all_transform_details[transform_identifier]

    # NOTE!!
    # remove the below once we add the next block of logic
    notify.data_engineering(f"""
                            
            RAN TO CURRENT COMPLETED STAGE!
                            
            transform details are:
                            
            {json.dumps(transform_details, indent=2, default=lambda x: str(x))}
                            """)
    

    # run transform to create csv+json from sdmx (or whatever source)
    # all the details you will need will be in transform_details

    # validate the metadata against the schema for dp-dataset-api v2

    # validate the csv using csv validation functions

    # upload the csv to dp-upload-service

    # upload any supplementary distributions to dp-upload-service

    # upload metadata to dp-dataset-api

    # notify PST that a dataset resource is ready for use by the CMS.
