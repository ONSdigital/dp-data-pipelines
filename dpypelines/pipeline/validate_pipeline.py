import json
from pathlib import Path

from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json.validation import validate_json_schema

from dpypelines.pipeline.models import Distribution, Manifest, Metadata
from dpypelines.pipeline.validation.utils import validate_file_format
from dpypelines.pipeline.validation.models import ValidationResult
from dpypelines.pipeline.validation.utils import validate_file_exists_and_not_empty

logger = DpLogger("data-ingress-pipeline")


def validate_manifest(
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


def load_and_validate_metadata(
    manifest: Manifest, local_store: LocalDirectoryStore
) -> Metadata:
    """
    Main validation function that returns validated objects.
    """
    # Retrieve and validate metadata.json
    metadata = load_metadata(manifest, local_store)

    # Retrieve and validate data files
    for distribution in metadata.distributions:
        validate_distribution_file(distribution, local_store)

    logger.info("Metadata and data files validated.")
    return metadata


def validate_distribution_file(
    distribution: Distribution, local_store: LocalDirectoryStore
) -> ValidationResult:
    logger.info(f"Validating distribution file {distribution.file}")
    file_path = local_store.local_path.absolute() / distribution.file

    validate_file_exists_and_not_empty(file_path)
    logger.info("Data file found", data={"data_file_name": distribution.file})

    validation_result = validate_file_format(file_path)
    if not validation_result.valid or validation_result.error:
        raise ValueError(
            f"File format validation failed for {distribution.file}: {validation_result.error}"
        )
    logger.info(
        f"Validated file format for {distribution.file}",
        data={"format": validation_result.format},
    )


def load_metadata(manifest: Manifest, local_store: LocalDirectoryStore) -> Metadata:
    """
    Validates the manifest file and then reads + deserialises JSON to Metadata instance.
    """
    validate_file_exists_and_not_empty(
        local_store.local_path.absolute() / manifest.metadata_file
    )

    metadata_dict = read_json_file(
        local_store.local_path.absolute() / manifest.metadata_file
    )

    return Metadata.model_validate(metadata_dict)


def read_json_file(file_path: Path) -> dict:
    """Validate and parse JSON file."""
    try:
        with open(file_path) as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"File {file_path} is not valid JSON: {str(e)}")
