from pathlib import Path

import pytest

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1

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
    }
    result = dataset_ingress_v1(files, pipeline_config)
    assert result is True


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
    }
    with pytest.raises(FileNotFoundError) as e:
        dataset_ingress_v1(files, pipeline_config)
    assert "No files found matching pattern: ^data.csv$" in str(e.value)


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
    }
    with pytest.raises(FileNotFoundError) as e:
        dataset_ingress_v1(files, pipeline_config)
    assert (
        "Required file not found: tests/fixtures/test-cases/dataset_ingress_v1/invalid_no_metadata/metadata.json"
        in str(e.value)
    )


# TODO Change assertion and reinstate test once validation of existence of manifest added
# def test_dataset_ingress_v1_manifest_missing():
#     """
#     Tests that `dataset_ingress_v1()` raises Exception if the manifest file is missing.
#     """
#     mp = pytest.MonkeyPatch()
#     mp.setenv("SKIP_DATA_UPLOAD", "True")

#     files = test_cases_base_dir / "invalid_no_manifest"
#     pipeline_config = {
#         "config_version": 1,
#         "transform": None,
#         "transform_inputs": {},
#         "transform_kwargs": {},
#         "required_files": [
#             {"matches": "^data.csv$"},
#             {"matches": "^metadata.json$"},
#         ],
#         "supplementary_distributions": {},
#     }
#     with pytest.raises(FileNotFoundError) as e:
#         dataset_ingress_v1(files, pipeline_config)
#     assert "Required file not found: tests/fixtures/test-cases/dataset_ingress_v1/invalid_no_manifest/manifest.json" in str(e.value)
