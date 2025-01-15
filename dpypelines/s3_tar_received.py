from pathlib import Path

from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import decompress_s3_tar
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json.validation import validate_json_schema

from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.utils import (
    get_notifier,
    get_secondary_function,
    get_source_id,
)

logger = DpLogger("data-ingress-pipeline")


def start(s3_object_name: str):
    """
    Handles the required behaviour when receiving a `.tar` file indicated by an s3 object name.

    Args:
        s3_object_name (str): The S3 object name of the tar file to be processed.
        Example: 'my-bucket/my-data.tar'

    """
    # TODO: Keep eye out for this. Might need to be reverted back to try and except format if issues arise.
    notifier = get_notifier()
    # DIS-2334 TODO: create a new function for error handling (consolidate logging, notification, and exceptions)

    # Decompress the tar file to the workspace
    try:
        decompress_s3_tar(s3_object_name, "input")
        logger.info(
            "S3 `.tar` object received and decompressed to ./input",
            data={"s3_object_name": s3_object_name},
        )
    except Exception as err:
        logger.error(
            "Failed to decompress S3 `.tar` object",
            err,
            data={"s3_object_name": s3_object_name},
        )
        notifier.failure()
        raise err

    # Create a local directory store using the decompressed files
    try:
        local_store = LocalDirectoryStore("input")
        logger.info(
            "Local directory store successfully created using decompressed files",
            data={"local_store_files": local_store.get_file_names()},
        )
    except Exception as err:
        logger.error(
            "Failed to create local directory store using decompressed files", err
        )
        notifier.failure()
        raise err

    try:
        manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")
        logger.info(
            "Successfully retrieved manifest.json file",
            data={
                "manifest_dict": manifest_dict,
            },
        )
    except Exception as err:
        logger.error("Failed to retrieve manifest.json file", err)
        notifier.failure()
        raise err

    # DIS-2334 TODO: Extract manifest validation into a dedicated function for clarity/reusability
    # This method will use a schema to validate the manifest.json
    try:
        file_path = Path(__file__).parent
        schema_path = Path(file_path / "schemas/manifest_v1_schema.json")
        validate_json_schema(
            schema_path=schema_path,
            data_dict=manifest_dict,
            error_msg="Invalid manifest",
        )
        logger.info(
            "Successfully validated manifest.json file",
            data={
                "manifest_dict": manifest_dict,
            },
        )
    except Exception as err:
        logger.error("Failed to validate manifest.json file", err)
        notifier.failure()
        raise err

    try:
        source_id = get_source_id(manifest_dict)
        logger.info("Successfully retrieved source_id", data={"source_id": source_id})
    except Exception as err:
        logger.error("Failed to retrieve source_id", err)
        notifier.failure()
        raise err

    # Get config details for the given source_id
    try:
        pipeline_config = get_pipeline_config(source_id)
        logger.info(
            "Successfully retrieved config details for given source_id",
            data={"pipeline_config": pipeline_config, "source_id": source_id},
        )
    except Exception as err:
        logger.error(
            "Failed to retrieve config details for given source_id",
            err,
            data={"source_id": source_id},
        )
        notifier.failure()
        raise err

    # Get the path to the directory
    files_dir = local_store.get_current_source_pathlike()

    # Call the secondary_function specified in pipeline_config
    try:
        secondary_function = get_secondary_function(pipeline_config)
        secondary_function(files_dir, pipeline_config)
        logger.info(
            "Successfully executed secondary function specified in pipeline_config",
            data={
                "secondary_function": secondary_function,
                "pipeline_config": pipeline_config,
            },
        )
    except Exception as err:
        logger.error(
            "Failed to executed secondary function specified in pipeline_config",
            err,
            data={"pipeline_config": pipeline_config, "files_dir": files_dir},
        )
        notifier.failure()
        raise err
