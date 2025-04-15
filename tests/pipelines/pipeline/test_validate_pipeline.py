import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pydantic_core import ValidationError

from dpypelines.pipeline.models import Manifest, Metadata, SubmissionContact
from dpypelines.pipeline.validate_pipeline import (
    retrieve_and_validate_manifest,
    validate_file_exists_and_not_empty,
    validate_json_file,
    validate_manifest_schema,
    validate_pipeline_files,
)

test_cases_base_dir = Path("tests/fixtures/test-cases")


@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_retrieve_and_validate_manifest(mock_local_store):
    mock_local_store = MagicMock(name="local_store")
    mock_local_store.get_lone_matching_json_as_dict.return_value = {
        "metadata_file": "metadata.json",
        "submission_contacts": [{"email": "jane.doe@ons.gov.uk"}],
    }
    manifest = retrieve_and_validate_manifest(mock_local_store)

    assert isinstance(manifest, Manifest)
    assert manifest.metadata_file == "metadata.json"
    assert manifest.submission_contacts[0].email == "jane.doe@ons.gov.uk"


@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_retrieve_and_validate_manifest_file_not_found(mock_local_store):
    mock_local_store = MagicMock(name="local_store")
    mock_local_store.get_lone_matching_json_as_dict.return_value = {}
    with pytest.raises(FileNotFoundError) as e:
        retrieve_and_validate_manifest(mock_local_store)
    assert "Failed to retrieve manifest from the local directory store." in str(e)


@patch("dpypelines.pipeline.validate_pipeline.validate_json_file")
@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_validate_pipeline_files(mock_local_store, mock_validate_json):
    """Test that `validate_pipeline_files()` returns a valid Metadata model."""
    mock_local_store = MagicMock(name="local_store")
    manifest = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
    )
    with open(test_cases_base_dir / "test_metadata.json", "r") as f:
        metadata_dict = json.load(f)

    mock_validate_json.return_value = metadata_dict

    metadata = validate_pipeline_files(manifest, mock_local_store)
    assert isinstance(metadata, Metadata)


@patch("dpypelines.pipeline.validate_pipeline.validate_json_file")
@patch("dpypelines.pipeline.validate_pipeline.LocalDirectoryStore")
def test_validate_pipeline_files_invalid_metadata(mock_local_store, mock_validate_json):
    """Test that `validate_pipeline_files()` returns a valid Metadata model."""
    mock_local_store = MagicMock(name="local_store")
    manifest = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
    )
    with open(test_cases_base_dir / "test_metadata_dataset_id_missing.json", "r") as f:
        metadata_dict = json.load(f)

    mock_validate_json.return_value = metadata_dict

    with pytest.raises(ValidationError) as e:
        validate_pipeline_files(manifest, mock_local_store)

    assert "1 validation error for Metadata\ndataset_id" in str(e)


def test_validate_manifest_schema_fails():
    manifest_dict = {"key": "value"}
    with pytest.raises(ValueError) as e:
        validate_manifest_schema(manifest_dict)
    assert "Manifest schema validation failed" in str(e)


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


def test_validate_json_file():
    """
    Tests that `validate_json_file()` returns the expected dictionary if a valid JSON file is provided.
    """
    file_path = test_cases_base_dir / "test_manifest.json"
    result = validate_json_file(file_path)

    assert isinstance(result, dict)
    assert "metadata_file" in result
    assert "submission_contacts" in result
    assert "email" in result["submission_contacts"][0].keys()


def test_validate_json_file_invalid():
    """
    Tests that `validate_json_file()` raises ValueError if the JSON file is invalid.
    """
    file_path = test_cases_base_dir / "invalid_json_file.json"
    file_path.write_text("invalid json")  # Write invalid JSON content

    with pytest.raises(ValueError) as e:
        validate_json_file(file_path)

    assert "File is not valid JSON" in str(e.value)
