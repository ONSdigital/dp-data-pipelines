
def ingress_v1():
    """
    Version one of the data ingress pipeline.
    """
    
    # ... the rough list of steps this pipeline will perform follow ...

    # IN ALL CASES, the FOLLOWING STEPS WILL BE IN TRY CATCH
    # BLOCKS AND WILL NOTIFY RELEVANT PARTIES IN EVENT OF FAILURE

    # decompress tar file to workspace

    # confirm we have a config

    # use config schema to confirm that config is valid

    # verify that the specified required files are in the tar file

    # verify that the specified supplementary distributions are in the tar file 

    # use config "pipeline" key to get the transform and sanity checking code for the source in question

    # run transform to create csv+json from sdmx (or whatever source)

    # validate the metadata against the schema for dp-dataset-api v2

    # validate the csv using csv validation functions

    # upload the csv to dp-upload-service

    # upload any supplementary distributions to dp-upload-service

    # upload metadata to dp-dataset-api

    # notify PST that a dataset resource is ready for use by the CMS.