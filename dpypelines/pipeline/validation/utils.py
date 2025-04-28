from pathlib import Path

from dpypelines.pipeline.validation.file_validators import get_file_validators
from dpypelines.pipeline.validation.models import ValidationResult


def validate_file_exists_and_not_empty(file_path: Path) -> None:
    """Ensure file exists and is not empty."""
    if not file_path.is_file():
        raise FileNotFoundError(f"Required file not found: {file_path.name}")
    if _file_is_empty(file_path):
        raise ValueError(f"File is empty: {file_path.name}")


def _file_is_empty(file_path: Path) -> bool:
    return file_path.stat().st_size == 0


def validate_file_format(file_path: Path) -> ValidationResult:
    """
    Validate that file has the correct extension and basic format characteristics.
    Performs content validation to ensure the file format matches its extension.

    Args:
        file_path: Path to the file to validate

    Returns:
        Dict with validation results including:
        - valid: Boolean indicating if file format is valid
        - format: The file format as a string
        - details: Additional format-specific details
    """
    validators = get_file_validators()

    for validator in validators:
        if validator.accepts_file(file_path):
            return validator.validate_file_format(file_path)

    extension = file_path.suffix.strip(".")
    return ValidationResult(False, extension, f"Extension {extension} is not supported")
