from pathlib import Path

import pytest

from dpypelines.pipeline.validate_pipeline import (
    validate_file_exists_and_not_empty,
    validate_json_file,
    validate_manifest_vars,
    validate_pattern_files,
    validate_pipeline_files,
)

test_cases_base_dir = Path("tests/fixtures/test-cases/dataset_ingress_v1")
pipeline_config = {
    "config_version": 1,
    "required_files": [
        {"matches": "^data.csv$"},
        {"matches": "^metadata.json$"},
    ],
    "supplementary_distributions": [],
}
pipeline_config_wsup = pipeline_config
pipeline_config_wsup["supplementary_distributions"] = [
    {"matches": "^supplementary.txt$"}
]


def test_validate_pipeline_files():
    """
    Tests that `validate_pipeline_files()` returns the expected dictionary if valid files are provided.
    """
    files = test_cases_base_dir / "valid"
    result = validate_pipeline_files(files, pipeline_config_wsup)
    assert "manifest" in result
    assert "input_files" in result
    assert "config_files" in result
    assert "supplementary_files" in result


def test_validate_pipeline_files_data_missing():
    """
    Tests that `validate_pipeline_files()` raises FileNotFoundError if the data file is missing.
    """
    files = test_cases_base_dir / "invalid_no_data"
    with pytest.raises(FileNotFoundError) as e:
        validate_pipeline_files(files, pipeline_config)

    assert "No files found matching pattern: ^data.csv$" in str(e.value)


def test_validate_pipeline_files_metadata_missing():
    """
    Tests that `validate_pipeline_files()` raises FileNotFoundError if the metadata file is missing.
    """
    files = test_cases_base_dir / "invalid_no_metadata"
    with pytest.raises(FileNotFoundError) as e:
        validate_pipeline_files(files, pipeline_config)

    assert (
        "Required file not found: tests/fixtures/test-cases/dataset_ingress_v1/invalid_no_metadata/metadata.json"
        in str(e.value)
    )


def test_validate_pipeline_files_supplementary_missing():
    """
    Tests that `validate_pipeline_files()` raises FileNotFoundError if the supplementary file is missing.
    """
    files = test_cases_base_dir / "invalid_no_supplementary"
    with pytest.raises(FileNotFoundError) as e:
        validate_pipeline_files(files, pipeline_config_wsup)

    assert "No files found matching pattern: ^supplementary.txt$" in str(e.value)


def test_validate_pipeline_files_manifest_missing():
    """
    Tests that `validate_pipeline_files()` raises FileNotFoundError if the manifest file is missing.
    """
    files = test_cases_base_dir / "invalid_no_manifest"
    with pytest.raises(FileNotFoundError) as e:
        validate_pipeline_files(files, pipeline_config)

    assert (
        "Required file not found: tests/fixtures/test-cases/dataset_ingress_v1/invalid_no_manifest/manifest.json"
        in str(e.value)
    )


def test_validate_pattern_files():
    """
    Tests that `validate_pattern_files()` returns the expected list of files if valid files are provided.
    """
    files = test_cases_base_dir / "valid"
    result = validate_pattern_files(files, pipeline_config, "required_files")
    assert len(result) == 2
    assert files / "data.csv" in result
    assert files / "metadata.json" in result


def test_validate_pattern_files_no_match():
    """
    Tests that `validate_pattern_files()` raises FileNotFoundError if no files match the pattern.
    """
    files = test_cases_base_dir / "invalid_no_data"
    pipeline_config = {
        "config_version": 1,
        "required_files": [
            {"matches": "^non_existent_file.csv$"},
        ],
    }
    with pytest.raises(FileNotFoundError) as e:
        validate_pattern_files(files, pipeline_config, "required_files")

    assert "No files found matching pattern: ^non_existent_file.csv$" in str(e.value)


def test_validate_file_exists_and_not_empty():
    """
    Tests that `validate_file_exists_and_not_empty()` raises FileNotFoundError if the file does not exist.
    """
    file_path = test_cases_base_dir / "non_existent_file.txt"
    with pytest.raises(FileNotFoundError) as e:
        validate_file_exists_and_not_empty(file_path)

    assert f"Required file not found: {file_path}" in str(e.value)


def test_validate_file_exists_and_not_empty_empty_file():
    """
    Tests that `validate_file_exists_and_not_empty()` raises ValueError if the file is empty.
    """
    file_path = test_cases_base_dir / "empty_file.txt"
    file_path.touch()  # Create an empty file
    with pytest.raises(ValueError) as e:
        validate_file_exists_and_not_empty(file_path)

    assert f"'{file_path}' is empty" in str(e.value)


def test_validate_json_file():
    """
    Tests that `validate_json_file()` returns the expected dictionary if a valid JSON file is provided.
    """
    file_path = test_cases_base_dir / "valid" / "manifest.json"
    result = validate_json_file(file_path)
    assert isinstance(result, dict)
    assert "manifestVersion" in result
    assert "source_id" in result
    assert "fileAuthorEmail" in result


def test_validate_json_file_invalid():
    """
    Tests that `validate_json_file()` raises ValueError if the JSON file is invalid.
    """
    file_path = test_cases_base_dir / "invalid_json_file.json"
    file_path.write_text("invalid json")  # Write invalid JSON content
    with pytest.raises(ValueError) as e:
        validate_json_file(file_path)

    assert "File is not valid JSON" in str(e.value)


def test_validate_manifest_vars():
    """
    Tests that `validate_manifest_vars()` raises KeyError if required keys are missing.
    """
    manifest_dict = {
        "manifestVersion": 1,
        "source_id": "test",
    }
    required_keys = ["manifestVersion", "source_id", "fileAuthorEmail"]
    with pytest.raises(KeyError) as e:
        validate_manifest_vars(manifest_dict, required_keys)

    assert "Missing required keys in manifest: fileAuthorEmail" in str(e.value)


def test_validate_manifest_vars_valid():
    """
    Tests that `validate_manifest_vars()` does not raise any exception if all required keys are present.
    """
    manifest_dict = {
        "manifestVersion": 1,
        "source_id": "test",
        "fileAuthorEmail": "test@valid.com",
    }
    required_keys = ["manifestVersion", "source_id", "fileAuthorEmail"]
    try:
        validate_manifest_vars(manifest_dict, required_keys)
    except KeyError:
        pytest.fail("validate_manifest_vars() raised KeyError unexpectedly!")
