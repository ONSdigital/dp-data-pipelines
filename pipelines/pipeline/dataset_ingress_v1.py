from pathlib import Path
from local_directory_store import LocalDirectoryStore
from pipelines.pipeline.shared.notification import data_engineering

def dataset_ingress_v1(files_dir: str):
    """
    Version one of the dataset ingress pipeline.

    files_dir: Path to the directory where the input
    files for this pipeline are located.
    """

    try:
        # create a LocalDirectoryStore object
        local_store = LocalDirectoryStore(files_dir)

        # verify that the specified required files have been provided
        required_files = local_store.get_required_files_patterns()
        if  not local_store.verify_required_file(required_files):
            raise ValueError("Required files not found in the input directory.")
        
        # verify that the specified supplementary distributions have been provided
        if not local_store.verify_supplementary_distribution():
            raise ValueError("Supplementary distributions not found in the input directory.")
        
        # use config "pipeline" key to get the transform and sanity checking code for the source in question
        pipeline_config = local_store.get_pipeline_config()
        if not pipeline_config:
            raise ValueError("Pipeline configuration not found in the input directory.")
        
    except Exception as e:
        # Notify data engineering in the event of an issue
        data_engineering(e)
        raise  
    
    # verify that the specified required files have been provided

    # verify that the specified supplementary distributions have been provided

    # use config "pipeline" key to get the transform and sanity checking code for the source in question

    # run transform to create csv+json from sdmx (or whatever source)

    # validate the metadata against the schema for dp-dataset-api v2

    # validate the csv using csv validation functions

    # upload the csv to dp-upload-service

    # upload any supplementary distributions to dp-upload-service

    # upload metadata to dp-dataset-api

    # notify PST that a dataset resource is ready for use by the CMS.
