import datetime
from unittest.mock import MagicMock, patch

import pytest
from requests import HTTPError

from dpypelines.pipeline.dataset_api import (
    dataset_type_is_static,
    upload_metadata,
)
from dpypelines.pipeline.errors import DatasetTypeException
from dpypelines.pipeline.models import Distribution, Metadata


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_upload_metadata_succeeds(mock_DatasetAPIClient):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.post_json.return_value.status_code = 200
    metadata = Metadata(
        edition_title="Edition title",
        distributions=[
            Distribution(title="Distribution title", format="csv", file="data.csv")
        ],
        release_date="2025-01-01T00:00:00",
        dataset_id="dataset-id",
        edition="edition-id",
    )
    metadata_uploaded = upload_metadata(metadata, mock_dataset_api_client)
    assert metadata_uploaded
    mock_dataset_api_client.post_json.assert_called_once_with(
        {
            "alerts": [],
            "distributions": [
                {
                    "download_url": "https://download.ons.gov.uk/data.csv",
                    "file": "data.csv",
                    "format": "csv",
                    "media_type": "text/csv",
                    "title": "Distribution title",
                }
            ],
            "edition_title": "Edition title",
            "quality_designation": None,
            "release_date": datetime.datetime(2025, 1, 1, 0, 0),
            "usage_notes": [],
        }
    )


@patch("dpypelines.pipeline.dataset_api.Metadata")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_upload_metadata_fails_404(mock_DatasetAPIClient, mock_metadata):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.post_json.return_value.status_code = 404
    mock_dataset_api_client.post_json.side_effect = HTTPError
    mock_metadata = MagicMock()
    mock_metadata.return_value = Metadata(
        edition_title="Edition title",
        distributions=[
            Distribution(title="Distribution title", format="csv", file="data.csv")
        ],
        release_date="2025-01-01T00:00:00",
        dataset_id="dataset-id",
        edition="edition-id",
    )
    with pytest.raises(HTTPError):
        upload_metadata(mock_metadata.return_value, mock_dataset_api_client)


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_is_static(mock_dataset_api_client):
    mock_dataset_api_client = MagicMock()
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.get.return_value.text = '{"current": {"type": "static"}}'
    mock_dataset_api_client.get.return_value.status_code = 200

    dataset_is_static = dataset_type_is_static(mock_dataset_api_client)

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

    dataset_is_static = dataset_type_is_static(mock_dataset_api_client)

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
        dataset_type_is_static(mock_dataset_api_client)

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
        dataset_type_is_static(mock_dataset_api_client)

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
        dataset_type_is_static(mock_dataset_api_client)

    assert "DatasetNotFoundException" in str(e)
    mock_dataset_api_client.get.assert_called_once_with(
        "http://dataset-api.url/dataset_id",
        headers=mock_dataset_api_client.token_auth.get_auth_header.return_value,
    )
    mock_dataset_api_client.get.return_value.raise_for_status.assert_called_once()
