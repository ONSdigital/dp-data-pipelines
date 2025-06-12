from pathlib import Path
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.validation.json.validation import validate_json_schema
from dpypelines.pipeline.metadata.metadata_models import Manifest

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
