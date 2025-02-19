import json
import os
import re
from pathlib import Path
from typing import Dict, List

from dpytools.validation.json.validation import validate_json_schema

from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.utils import get_submitter_email


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
    """Validate file exists and has content."""
    if not file_path.exists():
        raise FileNotFoundError(f"Required file not found: {file_path}")

    if os.stat(file_path).st_size == 0:
        raise ValueError(f"'{file_path}' is empty")


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
        file_path = Path(__file__).parent
        schema_path = Path(file_path / "schemas/manifest_v1_schema.json")
        validate_json_schema(
            schema_path=schema_path,
            data_dict=manifest_dict,
            error_msg="Invalid manifest",
        )
    except Exception as e:
        raise ValueError(f"Manifest schema validation failed: {str(e)}")


def retrieve_and_validate_manifest(manifest_path: Path, required_keys: list) -> dict:
    """Retrieve and validate the manifest.json file."""
    try:
        manifest_dict = validate_json_file(manifest_path)
        validate_manifest_vars(manifest_dict, required_keys)
        validate_manifest_schema(manifest_dict)
        return manifest_dict
    except Exception as e:
        raise ValueError(f"Failed to retrieve and validate manifest: {str(e)}")
