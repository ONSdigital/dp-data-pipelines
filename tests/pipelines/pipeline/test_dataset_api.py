import datetime
from unittest.mock import MagicMock, patch

import pytest
from requests import HTTPError

from dpypelines.pipeline.dataset_api import (
    dataset_type_is_static,
    dataset_versions_path_exists,
    upload_metadata,
    validate_and_upload_metadata,
)
from dpypelines.pipeline.errors import DatasetTypeException, DatasetNotFoundException
from dpypelines.pipeline.models import Distribution, Metadata


@patch("dpypelines.pipeline.dataset_api.upload_metadata")
@patch("dpypelines.pipeline.dataset_api.is_valid_dataset")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
@patch("dpypelines.pipeline.dataset_api.JobConfiguration")
def test_validate_and_upload_metadata_succeeds(
    mock_job_config, mock_DatasetAPIClient, mock_valid_dataset, mock_upload_metadata
):
    metadata = Metadata(
        edition_title="Edition title",
        distributions=[
            Distribution(title="Distribution title", format="csv", file="data.csv")
        ],
        release_date="2025-01-01T00:00:00",
        dataset_id="dataset-id",
        edition="edition-id",
    )

    mock_job_configuration = MagicMock(name="JobConfiguration")
    mock_job_configuration.dataset_api_url = "http://dataset-api.url"
    mock_job_config.return_value = mock_job_configuration

    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client

    mock_valid_dataset = MagicMock(name="is_valid_dataset")
    mock_valid_dataset.return_value = True

    mock_upload_metadata = MagicMock(name="upload_metadata")
    mock_upload_metadata.return_value = True

    metadata_uploaded = validate_and_upload_metadata(metadata)

    assert metadata_uploaded


@patch("dpypelines.pipeline.dataset_api.upload_metadata")
@patch("dpypelines.pipeline.dataset_api.is_valid_dataset")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
@patch("dpypelines.pipeline.dataset_api.JobConfiguration")
def test_validate_and_upload_metadata_fails_invalid_dataset(
    mock_job_config, mock_DatasetAPIClient, mock_valid_dataset, mock_upload_metadata
):
    metadata = Metadata(
        edition_title="Edition title",
        distributions=[
            Distribution(title="Distribution title", format="csv", file="data.csv")
        ],
        release_date="2025-01-01T00:00:00",
        dataset_id="dataset-id",
        edition="edition-id",
    )

    mock_job_configuration = MagicMock(name="JobConfiguration")
    mock_job_configuration.dataset_api_url = "http://dataset-api.url"
    mock_job_config.return_value = mock_job_configuration

    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client

    mock_valid_dataset = MagicMock(name="is_valid_dataset")
    mock_valid_dataset.return_value = False

    mock_upload_metadata = MagicMock(name="upload_metadata")
    mock_upload_metadata.return_value = False

    metadata_uploaded = validate_and_upload_metadata(metadata)

    assert not metadata_uploaded


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


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_upload_metadata_fails_http_error(mock_DatasetAPIClient):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.post_json.side_effect = HTTPError
    metadata = Metadata(
        edition_title="Edition title",
        distributions=[
            Distribution(title="Distribution title", format="csv", file="data.csv")
        ],
        release_date="2025-01-01T00:00:00",
        dataset_id="dataset-id",
        edition="edition-id",
    )
    with pytest.raises(HTTPError):
        upload_metadata(metadata, mock_dataset_api_client)


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_dataset_versions_path_exists(mock_DatasetAPIClient):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.edition_path = "edition_id"
    mock_dataset_api_client.get_path.return_value.status_code = 200
    path_exists = dataset_versions_path_exists(mock_dataset_api_client)
    assert path_exists


@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_dataset_versions_path_does_not_exist(mock_DatasetAPIClient):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_api_client.edition_path = "edition_id"
    mock_dataset_api_client.get_path.side_effect = HTTPError
    with pytest.raises(HTTPError):
        dataset_versions_path_exists(mock_dataset_api_client)


@patch("dpypelines.pipeline.dataset_api.get_dataset_id_path_response")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_dataset_type_is_static(mock_DatasetAPIClient, mock_dataset_id_path_response):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_id_path_response.return_value = {"current": {"type": "static"}}

    dataset_is_static = dataset_type_is_static(mock_dataset_api_client)

    assert dataset_is_static


@patch("dpypelines.pipeline.dataset_api.get_dataset_id_path_response")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_dataset_type_is_not_static(
    mock_DatasetAPIClient, mock_dataset_id_path_response
):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_id_path_response.return_value = {"current": {"type": "not-static"}}

    dataset_is_static = dataset_type_is_static(mock_dataset_api_client)

    assert not dataset_is_static


@patch("dpypelines.pipeline.dataset_api.get_dataset_id_path_response")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_dataset_type_is_missing(mock_DatasetAPIClient, mock_dataset_id_path_response):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_id_path_response.return_value = {"current": {"key": "value"}}

    with pytest.raises(DatasetTypeException) as e:
        dataset_type_is_static(mock_dataset_api_client)

    assert "DatasetTypeException" in str(e)


@patch("dpypelines.pipeline.dataset_api.get_dataset_id_path_response")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_current_key_missing(
    mock_DatasetAPIClient, mock_dataset_id_path_response
):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_id_path_response.return_value = {"key": {"key": "value"}}

    with pytest.raises(KeyError) as e:
        dataset_type_is_static(mock_dataset_api_client)

    assert "'current' not found in dataset_result keys" in str(e)


@patch("dpypelines.pipeline.dataset_api.get_dataset_id_path_response")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIClient")
def test_check_dataset_type_404(mock_DatasetAPIClient, mock_dataset_id_path_response):
    mock_dataset_api_client = MagicMock(name="DatasetAPIClient")
    mock_DatasetAPIClient.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = "dataset_id"
    mock_dataset_id_path_response.return_value = None
    mock_dataset_api_client.get.return_value.status_code = 404

    with pytest.raises(DatasetNotFoundException) as e:
        dataset_type_is_static(mock_dataset_api_client)

    assert "DatasetNotFoundException" in str(e)
