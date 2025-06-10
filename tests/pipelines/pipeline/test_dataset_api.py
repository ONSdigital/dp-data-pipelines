from unittest.mock import MagicMock, patch

import pytest
from requests import HTTPError
from dpytools.http.api import DatasetType
from dpypelines.pipeline.dataset_api import (
    is_valid_dataset,
    upload_metadata,
    validate_and_upload_metadata,
)
from dpypelines.pipeline.errors import (
    DatasetNotFoundException,
    CurrentDatasetNotFoundException,
)
from dpypelines.pipeline.models.metadata_models import Distribution, Metadata


@patch("dpypelines.pipeline.dataset_api.upload_metadata")
@patch("dpypelines.pipeline.dataset_api.is_valid_dataset")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_validate_and_upload_metadata_succeeds(
    mock_dataset_api_service, mock_valid_dataset, mock_upload_metadata
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

    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client

    mock_valid_dataset = MagicMock(name="is_valid_dataset")
    mock_valid_dataset.return_value = True

    mock_upload_metadata = MagicMock(name="upload_metadata")
    mock_upload_metadata.return_value = True

    metadata_uploaded = validate_and_upload_metadata(
        metadata,
        dataset_api_service=mock_dataset_api_client,
    )

    assert metadata_uploaded


@patch("dpypelines.pipeline.dataset_api.is_valid_dataset")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_validate_and_upload_metadata_fails_invalid_dataset(
    mock_dataset_api_service, mock_valid_dataset
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

    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client

    mock_valid_dataset.return_value = False

    metadata_uploaded = validate_and_upload_metadata(
        metadata,
        dataset_api_service=mock_dataset_api_client,
    )

    assert not metadata_uploaded


@patch("dpypelines.pipeline.dataset_api.upload_metadata")
@patch("dpypelines.pipeline.dataset_api.is_valid_dataset")
@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_validate_and_upload_metadata_fails_upload_error(
    mock_dataset_api_service, mock_valid_dataset, mock_upload_metadata
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

    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client

    mock_valid_dataset.return_value = True
    mock_upload_metadata.return_value = False

    metadata_uploaded = validate_and_upload_metadata(metadata, mock_dataset_api_client)

    assert not metadata_uploaded


@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_upload_metadata_succeeds(mock_dataset_api_service):
    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client
    mock_dataset_api_client.versions.create_version.return_value.status_code = 200
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
    mock_dataset_api_client.versions.create_version.assert_called_once_with(
        {
            "edition_title": "Edition title",
            "distributions": [
                {
                    "title": "Distribution title",
                    "format": "csv",
                    "file": "data.csv",
                    "download_url": "https://download.ons.gov.uk/data.csv",
                    "media_type": "text/csv",
                }
            ],
            "release_date": "2025-01-01T00:00:00",
            "quality_designation": None,
            "usage_notes": [],
            "alerts": [],
        },
        dataset_id="dataset-id",
        edition_id="edition-id",
    )


@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_upload_metadata_fails_http_error(mock_dataset_api_service):
    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client
    mock_dataset_api_client.versions.create_version.side_effect = HTTPError
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


test_dataset_types = [dataset_type for dataset_type in DatasetType.__members__]


@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
@pytest.mark.parametrize("dataset_type", test_dataset_types)
def test_is_valid_dataset_passes_valid_dataset(
    mock_dataset_api_service, dataset_type: DatasetType
):
    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"

    get_datasets_response = MagicMock()
    get_datasets_response.current.type = dataset_type
    mock_dataset_api_client.datasets.get_dataset.return_value = get_datasets_response

    is_valid = is_valid_dataset("dataset_id", mock_dataset_api_client)

    assert is_valid


@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_dataset_with_missing_current_dataset_fails(mock_dataset_api_service):
    testing_dataset_id = "dataset_id"

    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = testing_dataset_id

    get_datasets_response = MagicMock()
    get_datasets_response.current = None
    get_datasets_response.next = None
    mock_dataset_api_client.datasets.get_dataset.return_value = get_datasets_response

    with pytest.raises(CurrentDatasetNotFoundException) as e:
        is_valid_dataset(testing_dataset_id, mock_dataset_api_client)

    assert e.value.dataset_id == testing_dataset_id
    assert "current" in str(e.value)


@patch("dpypelines.pipeline.dataset_api.DatasetAPIService")
def test_check_dataset_type_404(mock_dataset_api_service):
    testing_dataset_id = "dataset_id"
    mock_dataset_api_client = MagicMock(name="DatasetAPIService")
    mock_dataset_api_service.return_value = mock_dataset_api_client
    mock_dataset_api_client.dataset_api_url = "http://dataset-api.url"
    mock_dataset_api_client.dataset_path = testing_dataset_id

    mock_dataset_api_client.datasets.get_dataset.return_value = None

    with pytest.raises(DatasetNotFoundException) as e:
        is_valid_dataset(testing_dataset_id, mock_dataset_api_client)

    assert e.value.dataset_id == testing_dataset_id
    assert "not found" in str(e.value)
