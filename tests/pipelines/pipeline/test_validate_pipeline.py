import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic_core import ValidationError

from dpypelines.pipeline.models import Manifest, Metadata, SubmissionContact
from dpypelines.pipeline.validate_pipeline import (
    validate_manifest,
    read_json_file,
    validate_manifest_schema,
    load_and_validate_metadata,
)
from dpypelines.pipeline.validation.models import ValidationResult

test_cases_base_dir = Path("tests/fixtures/test-cases")


@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_validate_manifest(mock_local_store):
    mock_local_store = MagicMock(name="local_store")
    mock_local_store.get_lone_matching_json_as_dict.return_value = {
        "metadata_file": "metadata.json",
        "submission_contacts": [{"email": "jane.doe@ons.gov.uk"}],
    }
    manifest = validate_manifest(mock_local_store)

    assert isinstance(manifest, Manifest)
    assert manifest.metadata_file == "metadata.json"
    assert manifest.submission_contacts[0].email == "jane.doe@ons.gov.uk"


@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_validate_manifest_file_not_found(mock_local_store):
    mock_local_store = MagicMock(name="local_store")
    mock_local_store.get_lone_matching_json_as_dict.return_value = {}
    with pytest.raises(FileNotFoundError) as e:
        validate_manifest(mock_local_store)
    assert "Failed to retrieve manifest from the local directory store." in str(e)


@patch("dpypelines.pipeline.validate_pipeline.validate_file_exists_and_not_empty")
@patch("dpypelines.pipeline.validate_pipeline.validate_file_format")
@patch("dpypelines.pipeline.validate_pipeline.read_json_file")
@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_load_and_validate_metadata(
    mock_local_store,
    mock_validate_json,
    mock_file_format_validator,
    mock_validate_file_exists_and_not_empty,
):
    """Test that `load_and_validate_metadata()` returns a valid Metadata model."""
    mock_local_store = MagicMock(name="local_store")
    mock_file_format_validator.return_value = ValidationResult(True, "csv")
    mock_validate_file_exists_and_not_empty.return_value = None
    manifest = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
    )
    with open(test_cases_base_dir / "test_metadata.json", "r") as f:
        metadata_dict = json.load(f)

    mock_validate_json.return_value = metadata_dict

    metadata = load_and_validate_metadata(manifest, mock_local_store)
    assert isinstance(metadata, Metadata)


@patch("dpypelines.pipeline.validate_pipeline.read_json_file")
@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_load_and_validate_metadata_invalid_metadata(
    mock_local_store, mock_validate_json
):
    """Test that `load_and_validate_metadata()` returns a valid Metadata model."""
    mock_local_store = MagicMock(name="local_store")
    manifest = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
    )
    with open(test_cases_base_dir / "test_metadata_dataset_id_missing.json", "r") as f:
        metadata_dict = json.load(f)

    mock_validate_json.return_value = metadata_dict

    with pytest.raises(ValidationError) as e:
        load_and_validate_metadata(manifest, mock_local_store)

    assert "1 validation error for Metadata\ndataset_id" in str(e)


def test_validate_manifest_schema_fails():
    manifest_dict = {"key": "value"}
    with pytest.raises(ValueError) as e:
        validate_manifest_schema(manifest_dict)
    assert "Manifest schema validation failed" in str(e)


def test_read_json_file():
    """
    Tests that `read_json_file()` returns the expected dictionary if a valid JSON file is provided.
    """
    file_path = test_cases_base_dir / "test_manifest.json"
    result = read_json_file(file_path)

    assert isinstance(result, dict)
    assert "metadata_file" in result
    assert "submission_contacts" in result
    assert "email" in result["submission_contacts"][0].keys()


def test_read_json_file_invalid():
    """
    Tests that `read_json_file()` raises ValueError if the JSON file is invalid.
    """
    file_path = test_cases_base_dir / "invalid_json_file.json"
    file_path.write_text("invalid json")  # Write invalid JSON content

    with pytest.raises(ValueError) as e:
        read_json_file(file_path)

    assert "not valid JSON" in str(e.value)
    assert str(file_path) in str(e.value)
