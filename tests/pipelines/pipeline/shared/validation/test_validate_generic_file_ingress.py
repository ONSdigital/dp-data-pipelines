from pathlib import Path
import pytest

from dpypelines.pipeline.validate_dataset_ingress_v1 import files_size_not_0, metadata_json_is_parseable

test_dir = Path(__file__).parents[4]
fixtures_files_dir = Path(test_dir / "fixtures/test-cases")

def test_import_files_size_not_0():
    """
    Checks that a given file with a size that is not 0 can be validated.
    """
    test_file = Path(fixtures_files_dir / "test_validate_csv_data.csv")

    test_result = files_size_not_0(test_file)

    # Validation function will raise nothing if it passes
    assert not test_result


def test_import_files_size_not_0_error():
    """
    Checks that a given file with a size that is 0 raises the expected error.
    """
    test_file = Path(fixtures_files_dir / "test_validate_file_size_0.txt")

    with pytest.raises(AssertionError) as err:
        files_size_not_0(test_file)
    
    assert str(err.value) == f"{test_file} is empty"


def test_metadata_json_is_parseable():
    """
    Checks that the given metadata.json file can be loaded.
    """
    test_file = Path(fixtures_files_dir / "test_manifest.json")

    test_result = metadata_json_is_parseable(test_file)

    # Validation function will raise nothing if it passes
    assert not test_result


def test_metadata_json_is_parseable_error():
    """
    Checks that the expected validation error will be raised if the given input file is not a parseable metadata.json file
    """
    test_file = Path(fixtures_files_dir / "test_validate_csv_data.csv")

    with pytest.raises(Exception) as err:
        test_result = metadata_json_is_parseable(test_file)
        
    
    assert str(err.value) == f"{test_file} is not parseable"

