import json
import re
from pathlib import Path
from typing import Dict, List

from dpytools.logging.logger import DpLogger
from dpytools.validation.json.validation import validate_json_schema

from dpypelines.pipeline.configuration import get_pipeline_config
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.utils import get_submitter_email

logger = DpLogger("data-ingress-pipeline")


def validate_pipeline_files(files_dir: Path, pipeline_config: dict) -> Dict:
    """
    Main validation function that returns validated objects.
    """
    required_keys = ["manifestVersion", "source_id", "fileAuthorEmail"]
    # 1. Check core required files
    required_files = ["metadata.json", "manifest.json"]
    for file_name in required_files:
        validate_file_exists_and_not_empty(files_dir / file_name)

    # 2. Retrieve and validate manifest.json and metadata.json
    manifest_dict = retrieve_and_validate_manifest(
        files_dir / "manifest.json", required_keys
    )
    metadata_dict = validate_json_file(files_dir / "metadata.json")

    # 3. Validate config-required files
    config_files = []
    config_files.extend(
        validate_pattern_files(files_dir, pipeline_config, "required_files")
    )

    # 4. Validate supplementary files
    supplementary_files = validate_pattern_files(
        files_dir, pipeline_config, "supplementary_distributions"
    )
    config_files.extend(supplementary_files)

    return {
        "manifest": manifest_dict,
        "metadata": metadata_dict,
        "input_files": config_files,
        "config_files": config_files,
        "supplementary_files": supplementary_files,
    }


def validate_pattern_files(
    files_dir: Path, pipeline_config: dict, pattern_key: str
) -> List[Path]:
    """Validate files matching a regex pattern exist and are not empty."""
    collected_files = []
    patterns = get_matching_pattern(pipeline_config, pattern_key)

    if patterns:
        for pattern in patterns:
            regex = re.compile(pattern)
            matched_files = [f for f in files_dir.iterdir() if regex.match(f.name)]
            if not matched_files:
                raise FileNotFoundError(f"No files found matching pattern: {pattern}")

            for file in matched_files:
                validate_file_exists_and_not_empty(file)
                collected_files.append(file)

    return collected_files


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


def validate_manifest_vars(manifest_dict: dict, required_keys: list) -> None:
    """Validate manifest dictionary has required fields."""
    missing_keys = [key for key in required_keys if key not in manifest_dict]

    if missing_keys:
        raise KeyError(f"Missing required keys in manifest: {', '.join(missing_keys)}")

    # Validate submitter email
    get_submitter_email(manifest_dict)


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


def retrieve_and_validate_manifest(manifest_path: Path, required_keys: list) -> dict:
    """Retrieve and validate the manifest.json file."""
    try:
        manifest_dict = validate_json_file(manifest_path)
        validate_manifest_vars(manifest_dict, required_keys)
        validate_manifest_schema(manifest_dict)
        return manifest_dict
    except Exception as e:
        raise ValueError(f"Failed to retrieve and validate manifest: {str(e)}")


def retrieve_config_and_files(local_store):
    """Retrieve configuration and files from the local directory."""
    manifest_dict = retrieve_manifest(local_store)
    source_id = get_source_id_from_manifest(manifest_dict)
    pipeline_config = get_pipeline_config_for_source(source_id)
    files_dir = Path(local_store.get_current_source_pathlike())

    if not manifest_dict or not source_id or not pipeline_config or not files_dir:
        err_msg = "Failed to retrieve configuration and files from the local directory."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)

    logger.info(
        "Configuration and files retrieved successfully",
        data={
            "manifest_dict": manifest_dict,
            "source_id": source_id,
            "pipeline_config": pipeline_config,
            "files_dir": str(files_dir),
        },
    )

    return manifest_dict, pipeline_config, files_dir


def retrieve_manifest(local_store):
    """Retrieve the manifest.json file from the local directory."""
    manifest_dict = local_store.get_lone_matching_json_as_dict("manifest.json")

    if not manifest_dict:
        err_msg = "manifest.json not found in the local store."
        logger.error(err_msg)
        raise FileNotFoundError(err_msg)
    return manifest_dict


def get_source_id(manifest_dict: dict) -> str:
    """
    This function returns the `source_id` form the provided manifest_dict (which is the data in the manifest.json file).
    """
    return manifest_dict["source_id"]


def get_source_id_from_manifest(manifest_dict):
    """Extract the source_id from the manifest."""
    source_id = get_source_id(manifest_dict)
    if not source_id:
        err_msg = f"source_id not found within manifest: {manifest_dict}."
        logger.error(err_msg)
        raise KeyError(err_msg)
    return source_id


def get_pipeline_config_for_source(source_id):
    """Retrieve pipeline configuration using source_id."""
    pipeline_config = get_pipeline_config(source_id)
    if not pipeline_config:
        err_msg = f"Pipeline configuration not found for source_id: {source_id}."
        logger.error(err_msg)
        raise ValueError(err_msg)
    return pipeline_config
