from pathlib import Path


# IMPORTANT
# We probably want to break these stages out a bit
# to help with reusibility
def dataset_ingress_v1(files_dir: Path):
    """
    Version one of the dataset ingress pipeline.

    files_dir: Path to the directory where the input
    files for this pipeline are located.
    """

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
