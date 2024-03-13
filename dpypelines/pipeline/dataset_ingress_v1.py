from pathlib import Path

from dpytools.stores.directory.local import LocalDirectoryStore
from dpypelines.pipeline.shared import notification as notify
from dpypelines.pipeline.shared.pipelineconfig import matching
from dpypelines.pipeline.functions.config import get_pipeline_identifier_from_config
from pipelines.pipeline.shared import message

def dataset_ingress_v1(files_dir: str) -> None:
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
        notify.data_engineering(message.unexpected_error(f"Failed to access local data at {files_dir}", err))
        raise

    # Load the pipeline configuration as a dictionary
    try:
        pipeline_config: dict = local_store.get_lone_matching_json_as_dict("pipeline-config.json")
    except Exception as err:
        notify.data_engineering(message.pipeline_input_exception({"pipeline-config.json": "expected"}, local_store, err))
        raise

    
    # Make sure pipeline_config contains a "pipeline" key
    try:
        pipeline = get_pipeline_identifier_from_config(pipeline_config)
    except KeyError:
        notify.data_engineering(message.expected_config_key_missing("Pipeline key not found in config", "pipeline", "dataset_ingress_v1"))
        raise ValueError(message.expected_config_key_missing("Pipeline key not found in config", "pipeline", "dataset_ingress_v1"))
    
    # Check for the existence of a pipeline configuration file
    try:
        if not local_store.has_lone_file_matching("pipeline-config.json"):
            notify.data_engineering(message.expected_local_file_missing("Pipeline config not found", "pipeline-config.json", "dataset_ingress_v1"))
            raise ValueError(message.expected_local_file_missing("Pipeline config not found", "pipeline-config.json", "dataset_ingress_v1"))
    except Exception as err:
        notify.data_engineering(message.unexpected_error("Failed to check for pipeline config", err))
        raise

    # Extract the patterns for required files from the pipeline configuration
    try:
        required_file_patterns = matching.get_required_files_patterns(pipeline_config)
    except Exception as err:
        notify.data_engineering(message.unexpected_error("Failed to get required files patterns", err))
        raise

    # Check for the existence of each required file
    for required_file in required_file_patterns:
        try:
            if not local_store.has_lone_file_matching(required_file):
                notify.data_engineering(message.expected_local_file_missing(f"Required file {required_file} not found", required_file, "dataset_ingress_v1"))
                raise ValueError(message.expected_local_file_missing(f"Required file {required_file} not found", required_file, "dataset_ingress_v1"))
        except Exception as err:
            notify.data_engineering(message.unexpected_error(f"Failed to check for required file {required_file}", err))
            raise

    # Extract the patterns for supplementary distributions from the pipeline configuration
    try:
        supplementary_distribution_patterns = matching.get_supplementary_distribution_patterns(pipeline_config)
    except Exception as err:
        notify.data_engineering(message.unexpected_error(f"Failed to get supplementary distribution patterns", err))
        raise

    # Check for the existence of each supplementary distribution
    for supplementary_distribution in supplementary_distribution_patterns:
        try:
            if not local_store.has_lone_file_matching(supplementary_distribution):
                notify.data_engineering(message.expected_local_file_missing(f"Supplementary distribution {supplementary_distribution} not found", supplementary_distribution, "dataset_ingress_v1"))
                raise ValueError(message.expected_local_file_missing(f"Supplementary distribution {supplementary_distribution} not found", supplementary_distribution, "dataset_ingress_v1"))
        except Exception as err:
            notify.data_engineering(message.unexpected_error(f"Failed to check for supplementary distribution {supplementary_distribution}", err))
            raise
        


    # run transform to create csv+json from sdmx (or whatever source)

    # validate the metadata against the schema for dp-dataset-api v2

    # validate the csv using csv validation functions

    # upload the csv to dp-upload-service

    # upload any supplementary distributions to dp-upload-service

    # upload metadata to dp-dataset-api

    # notify PST that a dataset resource is ready for use by the CMS.
