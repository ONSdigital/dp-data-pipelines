from pathlib import Path

from dpypelines.pipeline.validate_ingest_files import (
    file_size_0,
    metadata_json_is_parseable,
)

test_dir = Path(__file__).parents[4]
fixtures_files_dir = Path(test_dir / "fixtures/test-cases")


def test_import_file_size_not_0():
    """
    Checks that a given file with a size that is not 0 can be validated.
    """
    test_file = Path(fixtures_files_dir / "test_validate_csv_data.csv")

    assert file_size_0(test_file, False) is False


def test_import_file_size_not_0_error():
    """
    Checks that a given file with a size that is 0 raises the expected error.
    """
    test_file = Path(fixtures_files_dir / "test_validate_file_size_0.txt")

    assert file_size_0(test_file, False) is True


def test_metadata_json_is_parseable():
    """
    Checks that the given metadata.json file can be loaded.
    """
    test_file = Path(fixtures_files_dir / "test_metadata.json")

    assert metadata_json_is_parseable(test_file, False) is True


def test_metadata_json_is_parseable_error():
    """
    Checks that the expected validation error will be raised if the given input file is not a parseable metadata.json file
    """
    test_file = Path(fixtures_files_dir / "test_validate_csv_data.csv")

    assert metadata_json_is_parseable(test_file, False) is False
