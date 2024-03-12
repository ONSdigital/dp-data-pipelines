from dpytools.s3.basic import decompress_s3_tar
import os

from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.shared import notification as notify

def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Example s3_object_name: my-bucket/my-data.tar
    """

    # Decompress the tar to ./inputs
    try:
        decompress_s3_tar(s3_object_name, "inputs")
    except Exception as err:
        notify.data_engineering("MESSAGE")
        raise err

    # Identify the child directory we've just decompressed to within /inputs
    try:
        directories = [d for d in os.listdir("inputs") if os.path.isdir(os.path.join("inputs", d))]
        assert len(directories) == 1, (
            "Aborting, input directory has more than one directory within it"
        )
        decompressed_directory = directories[0]
    except Exception as err:
        notify.data_engineering("MESSAGE")
        raise err

    # Create a local directory store using our new files
    try:
        store = LocalDirectoryStore(f"inputs/{decompressed_directory}")
    except Exception as err:
        notify.data_engineering("MESSAGE")
        raise err
    
    notify.data_engineering("WORKED")
