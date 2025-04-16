from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

from dpypelines.pipeline.validation.models import ValidationResult
from dpypelines.pipeline.validation.utils import (
    validate_file_exists_and_not_empty,
    validate_file_format,
)

test_cases_base_dir = Path("tests/fixtures/test-cases")


def test_validate_file_exists_and_not_empty_file_does_not_exist():
    """
    Tests that `validate_file_exists_and_not_empty()` raises FileNotFoundError if the file does not exist.
    """
    file_path = test_cases_base_dir / "non_existent_file.txt"
    with pytest.raises(FileNotFoundError) as e:
        validate_file_exists_and_not_empty(file_path)
    assert "Required file not found: non_existent_file.txt" in str(e.value)


def test_validate_file_exists_and_not_empty_file_is_empty():
    """
    Tests that `validate_file_exists_and_not_empty()` raises ValueError if the file is empty.
    """
    file_path = test_cases_base_dir / "empty_file.txt"
    file_path.touch()  # Create an empty file
    with pytest.raises(ValueError) as e:
        validate_file_exists_and_not_empty(file_path)
    assert "File is empty: empty_file.txt" in str(e.value)


@patch("dpypelines.pipeline.validation.utils.get_file_validators")
def test_validate_file_format_finds_matching_validator_at_end(mock_get_file_validators):
    mock_txt_validator = create_mock_validator("txt", True)
    mock_other_validator = create_mock_validator("someotherfile", True)
    mock_csv_validator = create_mock_validator("csv", True)

    mock_get_file_validators.return_value = [
        mock_txt_validator,
        mock_other_validator,
        mock_csv_validator,
    ]
    file_path = test_cases_base_dir / "empty_file.csv"

    validation_result = validate_file_format(file_path)

    assert validation_result.valid
    assert validation_result.error is None
    assert validation_result.format == "csv"

    mock_txt_validator.accepts_file.assert_called_once_with(file_path)
    mock_txt_validator.validate_file_format.assert_not_called()
    mock_other_validator.accepts_file.assert_called_once_with(file_path)
    mock_other_validator.validate_file_format.assert_not_called()

    mock_csv_validator.accepts_file.assert_called_once_with(file_path)
    mock_csv_validator.validate_file_format.assert_called_once_with(file_path)


@patch("dpypelines.pipeline.validation.utils.get_file_validators")
def test_validate_file_format_finds_matching_validator_at_start(
    mock_get_file_validators,
):
    mock_txt_validator = create_mock_validator("txt", True)
    mock_csv_validator = create_mock_validator("csv", True)

    mock_get_file_validators.return_value = [mock_txt_validator, mock_csv_validator]
    file_path = test_cases_base_dir / "empty_file.txt"

    validation_result = validate_file_format(file_path)

    assert validation_result.valid
    assert validation_result.error is None
    assert validation_result.format == "txt"

    mock_txt_validator.accepts_file.assert_called_once_with(file_path)
    mock_txt_validator.validate_file_format.assert_called_once_with(file_path)
    mock_csv_validator.accepts_file.assert_not_called()
    mock_csv_validator.validate_file_format.assert_not_called()


@patch("dpypelines.pipeline.validation.utils.get_file_validators")
def test_validate_file_format_returns_failure(mock_get_file_validators):
    mock_txt_validator = create_mock_validator("txt", True)
    mock_csv_validator = create_mock_validator("csv", False)

    mock_get_file_validators.return_value = [mock_txt_validator, mock_csv_validator]
    file_path = test_cases_base_dir / "empty_file.csv"

    validation_result = validate_file_format(file_path)

    assert not validation_result.valid
    assert validation_result.error is not None
    assert validation_result.format == "csv"

    mock_txt_validator.accepts_file.assert_called_once_with(file_path)
    mock_txt_validator.validate_file_format.assert_not_called()
    mock_csv_validator.accepts_file.assert_called_once_with(file_path)
    mock_csv_validator.validate_file_format.assert_called_once_with(file_path)


@patch("dpypelines.pipeline.validation.utils.get_file_validators")
def test_validate_file_format_returns_error_when_none_matching(
    mock_get_file_validators,
):
    mock_txt_validator = create_mock_validator("txt", True)
    mock_csv_validator = create_mock_validator("csv", False)

    mock_get_file_validators.return_value = [mock_txt_validator, mock_csv_validator]
    extension = "someotherfile"
    file_path = test_cases_base_dir / f"empty_file.{extension}"

    validation_result = validate_file_format(file_path)

    assert not validation_result.valid
    assert validation_result.error == f"Extension {extension} is not supported"
    assert validation_result.format == "someotherfile"

    mock_txt_validator.accepts_file.assert_called_once_with(file_path)
    mock_txt_validator.validate_file_format.assert_not_called()
    mock_csv_validator.accepts_file.assert_called_once_with(file_path)
    mock_csv_validator.validate_file_format.assert_not_called()


def create_mock_validator(file_format: str, is_valid: bool) -> MagicMock:
    def accepts_file(file_path: Path):
        return file_path.suffix.strip(".") == file_format

    def validate_file_format(file_path: Path):
        if not accepts_file(file_path):
            raise ValueError(f"{file_path} is not of type {file_format}")

        if is_valid:
            return ValidationResult(True, file_format)

        return ValidationResult(False, file_format, "Error message")

    mock_validator = MagicMock()
    mock_validator.accepts_file.side_effect = accepts_file
    mock_validator.validate_file_format.side_effect = validate_file_format
    return mock_validator
