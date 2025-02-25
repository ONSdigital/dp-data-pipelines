import json

import pytest

from dpypelines.pipeline.utils import get_post_request_values_from_metadata


def test_get_post_request_values_from_valid_metadata():
    """
    Tests that the correct dataset_path, edition_path and request_body are returned from a valid metadata.json file
    """
    with open("tests/fixtures/test-cases/test_metadata.json", "r") as f:
        metadata = json.load(f)
    dataset_path, edition_path, request_body = get_post_request_values_from_metadata(
        metadata
    )
    assert dataset_path == "trade"
    assert edition_path == "time-series"
    assert request_body["title"] == "Dataset title"
    assert "DistributionFormatEnum" in list(request_body["downloads"].keys())


def test_get_post_request_values_from_invalid_metadata():
    """
    Tests that an error is raised if attempting to get request parameters with an invalid metadata.json file.
    """
    with open("tests/fixtures/test-cases/test_metadata_invalid.json", "r") as f:
        metadata = json.load(f)

    with pytest.raises(KeyError) as e:
        get_post_request_values_from_metadata(metadata)

    assert "KeyError('dcterms:identifier')" in str(e)
