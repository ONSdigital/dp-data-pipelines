import json
from pathlib import Path

from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json.validation import validate_json_schema

from dpypelines.pipeline.models import Manifest, Metadata

logger = DpLogger("data-ingress-pipeline")


def retrieve_and_validate_manifest(
    local_store: LocalDirectoryStore,
) -> Manifest:
    """Retrieve and validate manifest from the local directory store."""
    manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")

    if not manifest_dict:
        err_msg = "Failed to retrieve manifest from the local directory store."
        raise FileNotFoundError(err_msg)

    # Validate manifest against JSON schema
    validate_manifest_schema(manifest_dict)

    logger.info(
        "Manifest retrieved and validated against schema.",
        data={
            "manifest_dict": manifest_dict,
        },
    )

    # Create Manifest object
    manifest = Manifest.model_validate(manifest_dict)
    return manifest


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

    # Create Metadata model from metadata_dict
    metadata = Metadata.model_validate(metadata_dict)

    # Retrieve and validate data files
    for distribution in metadata.distributions:
        validate_file_exists_and_not_empty(
            local_store.local_path.absolute() / distribution.file
        )
        logger.info("Data file found", data={"data_file_name": distribution.file})

    logger.info("Metadata and data files validated.")
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
