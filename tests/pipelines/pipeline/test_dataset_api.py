import json
from unittest.mock import MagicMock, patch

import pytest

from dpypelines.pipeline.dataset_api import (
    check_dataset_type_is_static,
    get_post_request_values_from_metadata,
)
from dpypelines.pipeline.errors import (
    DatasetAPIRequestCreationException,
    DatasetTypeException,
)


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
    assert ["title", "download_url", "byte_size", "format", "media_type"] == list(
        request_body["distributions"][0].keys()
    )


def test_get_post_request_values_from_invalid_metadata():
    """
    Tests that an error is raised if attempting to get request parameters with an invalid metadata.json file.
    """
    with open("tests/fixtures/test-cases/test_metadata_invalid.json", "r") as f:
        metadata = json.load(f)

    with pytest.raises(DatasetAPIRequestCreationException) as e:
        get_post_request_values_from_metadata(metadata)

    assert "DatasetAPIRequestCreationException" in str(e)


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_is_static(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"current": {"type": "static"}}'
    mock_dataset_api_client.get.return_value.status_code = 200
    mock_email_client = MagicMock()
    submitter_email = "test@example.com"

    dataset_is_static = check_dataset_type_is_static(
        mock_dataset_api_client, mock_email_client, submitter_email
    )

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
    mock_email_client = MagicMock()
    submitter_email = "test@example.com"

    with pytest.raises(DatasetTypeException) as e:
        check_dataset_type_is_static(
            mock_dataset_api_client, mock_email_client, submitter_email
        )

    assert "DatasetTypeException" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )
    mock_email_client.send.assert_called_once_with(
        "test@example.com",
        "Dataset Ingest: Metadata Submission Failed",
        "The metadata for dataset_id could not be submitted to the Dataset API. Submission failure details: Dataset type is not static: metadata not submitted to Dataset API. Dataset type: not-static",
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_is_missing(mock_dataset_api_client):
    mock_email_client = MagicMock()
    submitter_email = "test@example.com"
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"current": {"key": "value"}}'
    mock_dataset_api_client.get.return_value.status_code = 200

    with pytest.raises(DatasetTypeException) as e:
        check_dataset_type_is_static(
            mock_dataset_api_client, mock_email_client, submitter_email
        )

    assert "DatasetTypeException" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )
    mock_email_client.send.assert_called_once_with(
        "test@example.com",
        "Dataset Ingest: Metadata Submission Failed",
        "The metadata for dataset_id could not be submitted to the Dataset API. Submission failure details: Dataset type is not static: metadata not submitted to Dataset API. Dataset type: None",
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_current_missing(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"key": {"key": "value"}}'
    mock_dataset_api_client.get.return_value.status_code = 200
    mock_email_client = MagicMock()
    submitter_email = "test@example.com"

    with pytest.raises(KeyError) as e:
        check_dataset_type_is_static(
            mock_dataset_api_client, mock_email_client, submitter_email
        )

    assert "'current' not found in dataset_result keys" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_404(mock_dataset_api_client):
    mock_email_client = MagicMock()
    submitter_email = "test@example.com"
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"key": {"key": "value"}}'
    mock_dataset_api_client.get.return_value.status_code = 404

    check_dataset_type_is_static(
        mock_dataset_api_client, mock_email_client, submitter_email
    )

    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )
    mock_dataset_api_client.get.return_value.raise_for_status.assert_called_once()
