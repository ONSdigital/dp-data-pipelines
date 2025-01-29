from pathlib import Path
from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.generic_file_ingress_v1 import generic_file_ingress_v1

import pytest

test_cases_base_dir = Path("tests/fixtures/test-cases/dataset_ingress_v1")


def test_dataset_ingress_v1():
    """
    Tests that `dataset_ingress_v1()` returns True if valid data, metadata and manifest files are provided.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_DATA_UPLOAD", "True")

    files = test_cases_base_dir / "valid"
    pipeline_config = {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.csv$"},
            {"matches": "^metadata.json$"},
        ],
        "supplementary_distributions": {},
        "secondary_function": dataset_ingress_v1,
    }
    assert dataset_ingress_v1(files, pipeline_config) is True


def test_dataset_ingress_v1_data_missing():
    """
    Tests that `dataset_ingress_v1()` raises Exception if the data file is missing.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_DATA_UPLOAD", "True")

    files = test_cases_base_dir / "invalid_no_data"
    pipeline_config = {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.csv$"},
            {"matches": "^metadata.json$"},
        ],
        "supplementary_distributions": {},
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(Exception) as e:
        dataset_ingress_v1(files, pipeline_config)

    assert "Required file not found" in str(e)


def test_dataset_ingress_v1_metadata_missing():
    """
    Tests that `dataset_ingress_v1()` raises Exception if the metadata file is missing.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_DATA_UPLOAD", "True")

    files = test_cases_base_dir / "invalid_no_metadata"
    pipeline_config = {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.csv$"},
            {"matches": "^metadata.json$"},
        ],
        "supplementary_distributions": {},
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(Exception) as e:
        dataset_ingress_v1(files, pipeline_config)

    assert "Required file not found" in str(e)


def test_dataset_ingress_v1_manifest_missing():
    """
    Tests that `dataset_ingress_v1()` raises Exception if the manifest file is missing.
    """
    mp = pytest.MonkeyPatch()
    mp.setenv("SKIP_DATA_UPLOAD", "True")

    files = test_cases_base_dir / "invalid_no_manifest"
    pipeline_config = {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.csv$"},
            {"matches": "^metadata.json$"},
        ],
        "supplementary_distributions": {},
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(Exception) as e:
        dataset_ingress_v1(files, pipeline_config)
    # TODO Change assertion once validation of existence of manifest added
    assert "Failed to create email client" in str(e)
