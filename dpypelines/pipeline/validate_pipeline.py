import json
import re
from pathlib import Path
from typing import Dict, List, Tuple

from dpytools.logging.logger import DpLogger
from dpytools.validation.json.validation import validate_json_schema
from dpytools.stores.directory.local import LocalDirectoryStore
from dpypelines.pipeline.errors import ValidationException

from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.messages.utils import get_submitter_email
from dpypelines.pipeline.models import (
    Manifest,
    Metadata,
    get_manifest_from_json,
    get_metadata_from_json,
)


logger = DpLogger("data-ingress-pipeline")


def retrieve_and_validate_manifest(
    local_store: LocalDirectoryStore,
) -> Manifest:
    """Retrieve and validate manifest from the local directory store."""
    manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")

    if not manifest_dict:
        err_msg = "Failed to retrieve manifest from the local directory store."
        raise FileNotFoundError(err_msg)

    logger.info(
        "Manifest retrieved.",
        data={
            "manifest_dict": manifest_dict,
        },
    )

    # Validate manifest against JSON schema
    validate_manifest_schema(manifest_dict)

    # Create Manifest object
    manifest = get_manifest_from_json(manifest_dict)
    return manifest


def validate_pipeline_files(
    manifest: Manifest, local_store: LocalDirectoryStore
) -> Metadata:
    """
    Main validation function that returns validated objects.
    """
    # Retrieve and validate metadata.json
    validate_file_exists_and_not_empty(
        local_store.local_path.absolute() / manifest.metadata_file
    )
    metadata_dict = validate_json_file(
        local_store.local_path.absolute() / manifest.metadata_file
    )
    # 2885 Add schema validation for metadata.json?
    metadata = get_metadata_from_json(metadata_dict)

    # Retrieve and validate data files
    distributions = metadata.distributions
    for distribution in distributions:
        validate_file_exists_and_not_empty(
            local_store.local_path.absolute() / distribution.file
        )
        logger.info("Data file found", data={"data_file_name": distribution.file})

    return metadata


def validate_file_exists_and_not_empty(file_path: Path) -> None:
    """Ensure file exists and is not empty."""
    if not file_path.is_file():
        raise FileNotFoundError(f"Required file not found: {file_path.name}")
    if file_path.stat().st_size == 0:
        raise ValueError(f"File is empty: {file_path.name}")


def validate_json_file(file_path: Path) -> dict:
    """Validate and parse JSON file."""
    try:
        with open(file_path) as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"File is not valid JSON: {str(e)}")


def validate_manifest_schema(manifest_dict: dict) -> None:
    """Validate manifest dictionary against the schema."""
    try:
        file_path = Path(__file__).parent.parent
        schema_path = file_path / "schemas" / "manifest_v1_schema.json"
        validate_json_schema(
            schema_path=schema_path,
            data_dict=manifest_dict,
            error_msg="Invalid manifest",
        )
    except Exception as e:
        raise ValueError(f"Manifest schema validation failed: {e}")
