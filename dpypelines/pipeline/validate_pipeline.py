import json
from pathlib import Path
from typing import Dict, List, Tuple

from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.pipelineconfig.transform import get_transform_details
from dpypelines.pipeline.shared.utils import get_submitter_email
from dpypelines.pipeline.validate_ingest_files import (
    file_size_0,
    metadata_json_is_parseable,
)

logger = DpLogger("data-ingress-pipelines")


def validate_json_file(file_path: Path) -> dict:
    """Validate and parse JSON file."""
    try:
        with open(file_path) as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON format", data={"file_path": str(file_path)})
        raise ValueError(f"File is not valid JSON: {str(e)}")


def validate_file_exists_and_not_empty(file_path: Path) -> None:
    """Validate file exists and has content."""
    if not file_path.exists():
        logger.error("Required file not found", data={"file_path": str(file_path)})
        raise FileNotFoundError(f"Required file not found: {file_path}")

    if file_size_0(file_path, give_error=True):
        logger.error("File is empty", data={"file_path": str(file_path)})
        raise ValueError(f"Required file is empty: {file_path}")


def validate_manifest_vars(manifest_dict: dict) -> None:
    """Validate manifest dictionary has required fields."""
    required_keys = ["manifestVersion", "source_id", "fileAuthorEmail"]
    missing_keys = [key for key in required_keys if key not in manifest_dict]

    if missing_keys:
        logger.error(
            "Missing required manifest keys",
            data={
                "missing_keys": missing_keys,
                "manifest_keys": list(manifest_dict.keys()),
            },
        )
        raise KeyError(f"Missing required keys in manifest: {', '.join(missing_keys)}")

    # Validate submitter email
    try:
        get_submitter_email(manifest_dict)
    except Exception as e:
        logger.error("Invalid submitter email", data={"manifest": manifest_dict})
        raise ValueError(f"Invalid submitter email: {str(e)}")


def validate_transform_inputs(files_dir: Path, pipeline_config: dict) -> List[Path]:
    """Validate transform inputs and run sanity checks."""
    input_file_paths = []
    transform_inputs = get_transform_details(pipeline_config, "transform_inputs")

    for pattern, sanity_checker in transform_inputs.items():
        file_path = files_dir / pattern
        validate_file_exists_and_not_empty(file_path)
        try:
            sanity_checker(file_path)
            logger.info("Sanity check passed", data={"file": str(file_path)})
            input_file_paths.append(file_path)
        except Exception as e:
            logger.error("Sanity check failed", data={"file": str(file_path)})
            raise ValueError(f"Sanity check failed for {file_path}: {str(e)}")

    return input_file_paths


def validate_supplementary_files(files_dir: Path, pipeline_config: dict) -> List[Path]:
    """Validate supplementary distribution files."""
    supp_files = []
    patterns = get_matching_pattern(pipeline_config, "supplementary_distributions")

    if patterns:
        for pattern in patterns:
            file_path = files_dir / pattern
            validate_file_exists_and_not_empty(file_path)
            supp_files.append(file_path)

    return supp_files


def validate_pipeline(files_dir: Path, pipeline_config: dict) -> Dict:
    """Main validation function that returns validated objects."""
    logger.info("Starting pipeline validation", data={"files_dir": str(files_dir)})

    # 1. Check core required files
    required_files = ["metadata.json", "manifest.json"]
    for file_name in required_files:
        validate_file_exists_and_not_empty(files_dir / file_name)

    # 2. Validate manifest.json and metadata.json
    manifest_dict = validate_json_file(files_dir / "manifest.json")
    validate_manifest_vars(manifest_dict)
    metadata_dict = validate_json_file(files_dir / "metadata.json")

    # 3. Validate transform inputs
    input_paths = validate_transform_inputs(files_dir, pipeline_config)

    # 4. Validate config-required files
    config_files = []
    for pattern in get_matching_pattern(pipeline_config, "required_files"):
        file_path = files_dir / pattern
        validate_file_exists_and_not_empty(file_path)
        config_files.append(file_path)

    # 5. Validate supplementary files
    supp_files = validate_supplementary_files(files_dir, pipeline_config)

    logger.info("Pipeline validation completed successfully")

    return {
        "manifest": manifest_dict,
        "metadata": metadata_dict,
        "input_files": input_paths,
        "config_files": config_files,
        "supplementary_files": supp_files,
    }
