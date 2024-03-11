from pathlib import Path
from local_directory_store import LocalDirectoryStore
from dpypelines.pipeline.shared.notification import data_engineering

def dataset_ingress_v1(files_dir: str) -> None:
    """
    Version one of the dataset ingress pipeline.

    Args:
        files_dir (str): Path to the directory where the input files for this pipeline are located.

    Raises:
        ValueError: If required files, supplementary distributions, or pipeline configuration are not found in the input directory.
        Exception: If any other error occurs.
    """
    try:
        # create a LocalDirectoryStore object
        local_store = LocalDirectoryStore(files_dir)

        # verify that the specified required files have been provide
        if not local_store.verify_required_file():
            raise ValueError("Required files not found in the input directory.")
        
        # verify that the specified supplementary distributions have been provided
        if not local_store.verify_supplementary_distribution():
            raise ValueError("Supplementary distributions not found in the input directory.")
        
        # use config "pipeline" key to get the transform and sanity checking code for the source in question
        pipeline_config = local_store.get_pipeline_config()
        if not pipeline_config:
            raise ValueError("Pipeline configuration not found in the input directory.")
        
    except ValueError as value_error:
        # Notify data engineering in the event of an issue
        data_engineering(value_error)
        raise

    except Exception as general_error:
        # Notify data engineering in the event of an issue
        data_engineering(general_error)
        raise 

    # run transform to create csv+json from sdmx (or whatever source)

    # validate the metadata against the schema for dp-dataset-api v2

    # validate the csv using csv validation functions

    # upload the csv to dp-upload-service

    # upload any supplementary distributions to dp-upload-service

    # upload metadata to dp-dataset-api

    # notify PST that a dataset resource is ready for use by the CMS.
