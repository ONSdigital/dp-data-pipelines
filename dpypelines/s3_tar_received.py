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

    print(os.environ.keys())
    submitted_dir = s3_object_name.split("/")[-1].rstrip(".tar")

    notify.data_engineering("Sanity check 1 passed: Glue pipeline can notify webhook")

    try:
        decompress_s3_tar(s3_object_name, ".")
        notify.data_engineering("Sanity check 2 passed: We can decompress the submitted tar file")
    except Exception as err:
        notify.data_engineering(f"XXX Failed to decompress from s3. Go poke glue logs. XXX. Error {err}")
        raise err

    try:
        store = LocalDirectoryStore(submitted_dir")
        notify.data_engineering("Sanity check 3 passed: We can access the extracted data via a LocalDirectoryStore")
    except Exception as err:
        notify.data_engineering(f"XXX Failed to create local directory store from extracted files. Go poke glue logs. XXX. Error {err}")
        raise err

    try:
        files = store.get_file_names()
        files_as_text_list = "\n".join(files)
        notify.data_engineering(f"""
            All sanity checks passed.
                                
            We have unpacked the s3 tar from the create event to a local directory store.
                                
            Files received are:
                                
            {files_as_text_list}
                                """)
    except Exception as err:
        notify.data_engineering(f"XXX Failed to list files in local directory store. Go poke glue logs. XXX. Error {err}")
        raise err
