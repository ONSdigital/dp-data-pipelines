from pathlib import Path

import pytest
from dpypelines.pipeline.shared.files.json_file_readers import read_json_file


test_cases_base_dir = Path("tests/fixtures/test-cases")


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
