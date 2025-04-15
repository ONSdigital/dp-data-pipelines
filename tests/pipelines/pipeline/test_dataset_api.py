import json
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.dataset_api import (
    check_dataset_type_is_static,
    get_post_request_values_from_metadata,
)
from dpypelines.pipeline.errors import DatasetTypeException
from dpypelines.pipeline.models import Metadata


def test_get_post_request_values_from_valid_metadata():
    """
    Tests that the correct dataset_path, edition_path and request_body are returned from a valid Metadata model.
    """
    with open("tests/fixtures/test-cases/test_metadata.json", "r") as f:
        metadata_json = json.load(f)
    metadata = Metadata.model_validate(metadata_json)

    dataset_path, edition_path, request_body = get_post_request_values_from_metadata(
        metadata
    )

    assert dataset_path == "test-static-dataset-1"
    assert edition_path == "test-edition-1"
    assert request_body["edition_title"] == "July to September 2022"
    assert list(request_body["distributions"][0].keys()) == [
        "title",
        "format",
        "file",
        "download_url",
        "media_type",
    ]


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_is_static(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"current": {"type": "static"}}'
    mock_dataset_api_client.get.return_value.status_code = 200

    dataset_is_static = check_dataset_type_is_static(mock_dataset_api_client)

    assert dataset_is_static
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_is_not_static(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = (
        '{"current": {"type": "not-static"}}'
    )
    mock_dataset_api_client.get.return_value.status_code = 200

    dataset_is_static = check_dataset_type_is_static(mock_dataset_api_client)

    assert not dataset_is_static
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_is_missing(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"current": {"key": "value"}}'
    mock_dataset_api_client.get.return_value.status_code = 200

    with pytest.raises(DatasetTypeException) as e:
        check_dataset_type_is_static(mock_dataset_api_client)

    assert "DatasetTypeException" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_current_missing(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"key": {"key": "value"}}'
    mock_dataset_api_client.get.return_value.status_code = 200

    with pytest.raises(KeyError) as e:
        check_dataset_type_is_static(mock_dataset_api_client)

    assert "'current' not found in dataset_result keys" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_404(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock(name="dataset_api_client")
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"key": {"key": "value"}}'
    mock_dataset_api_client.get.return_value.status_code = 404

    with pytest.raises(Exception) as e:
        check_dataset_type_is_static(mock_dataset_api_client)

    assert "DatasetNotFoundException" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )
    mock_dataset_api_client.get.return_value.raise_for_status.assert_called_once()
