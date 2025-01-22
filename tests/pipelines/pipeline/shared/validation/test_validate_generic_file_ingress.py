from pathlib import Path
import pytest

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


def test_import_file_size_not_0_true():
    """
    Checks that a given file with a size that is 0 returns True
    """
    test_file = Path(fixtures_files_dir / "test_validate_file_size_0.txt")

    assert file_size_0(test_file, False) is True


def test_import_file_size_not_0_give_error():
    """
    Checks that a given file with a size that is 0 returns the expected error when the give_error argument is set.
    """
    test_file = Path(fixtures_files_dir / "test_validate_file_size_0.txt")
    with pytest.raises(ValueError) as e:
        file_size_0(test_file, True)

    assert (
        f"'{test_file}' is empty"
        == str(e.value)
    )


def test_metadata_json_is_parseable():
    """
    Checks that the given metadata.json file can be loaded.
    """
    test_file = Path(fixtures_files_dir / "test_metadata.json")

    assert metadata_json_is_parseable(test_file, False) is True


def test_metadata_json_is_parseable_false():
    """
    Checks that False will be returned if the given input file is not a parseable metadata.json file
    """
    test_file = Path(fixtures_files_dir / "test_validate_csv_data.csv")

    assert metadata_json_is_parseable(test_file, False) is False


def test_metadata_json_is_parseable_error():
    """
    Checks that the expected error will be raised if the given input file is not a parseable metadata.json file
    and the argument give_error is given.
    """
    test_file = Path(fixtures_files_dir / "test_validate_csv_data.csv")
    with pytest.raises(Exception) as e:
        metadata_json_is_parseable(test_file, True)

    assert (
        "metadata.json is not parseable"
        == str(e.value)
    )