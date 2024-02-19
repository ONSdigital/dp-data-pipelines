
# triggered by: ../s3_tar_recieved_trigger.yml
def s3_tar_received():
    """
    Handles the required behaviour when recieving a tar file via an
    s3 url.
    """

    # IN ALL CASES, the FOLLOWING STEPS WILL BE IN TRY CATCH
    # BLOCKS AND WILL NOTIFY RELEVANT PARTIES IN EVENT OF FAILURE

    # decompress tar file to workspace

    # confirm we have a config

    # if we dont have a config - make one? (tbd)

    # use config schema to confirm that config is valid

    # use the config info to select and run the appropriate pipeline,
    # there should literally by a field in the pipeline-config telling
    # us which pipeline from ./pipeline/* to call.
