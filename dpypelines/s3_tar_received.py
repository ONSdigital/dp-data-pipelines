from pathlib import Path

from dpytools.stores.directory.local import LocalDirectoryStore
from dpypelines.pipeline.shared import notification as notify
from dpypelines.pipeline.shared.pipelineconfig import matching
from dpypelines.pipeline.functions.schemas import get_config_schema_path
from dpytools.s3.basic import decompress_s3_tar
from dpytools.validation.json import validation

def start(s3_object_name: str):
    """
    Handles the required behaviour when recieving a tar file indicated
    by an s3 object name.

    Example s3_object_name: my-bucket/my-data.tar
    """
    try:
        # IN ALL CASES, the FOLLOWING STEPS WILL BE IN TRY CATCH
        # BLOCKS AND WILL NOTIFY RELEVANT PARTIES IN EVENT OF FAILURE

        # decompress tar file to workspace
        files_path = Path("./")
        contents = decompress_s3_tar(s3_object_name, files_path)
        localStore = LocalDirectoryStore(files_path)
    except Exception as error:
        notify.data_engineering("ERROR MESSAGE")
        raise error

        # confirm we have a config
    try:
        if not localStore.has_lone_file_matching("config.json"):
            notify.data_engineering("ERROR MESSAGE")
            raise ValueError("Missing Config file!")
    except Exception as error:
        notify.data_engineering("ERROR MESSAGE")
        raise error

        # if we dont have a config - make one? (tbd)

        # use config schema to confirm that config is valid
    try:
        config_dict = localStore.get_lone_matching_json_as_dict("path_to_config_file")
        path_to_schema = get_config_schema_path(config_dict)
        validation.validate_json_schema(schema_path=path_to_schema, data_dict=config_dict)
    except Exception as error:
        notify.data_engineering("ERROR MESSAGE")
        raise error

    
        # use the config info to select and run the appropriate pipeline,
        
        # there should literally by a field in the pipeline-config telling
        # us which pipeline from ./pipeline/* to call.
