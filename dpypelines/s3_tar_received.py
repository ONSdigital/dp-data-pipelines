def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Example s3_object_name: my-bucket/my-data.tar
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
