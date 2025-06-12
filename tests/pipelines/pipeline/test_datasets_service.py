from unittest.mock import MagicMock, patch

from dpytools.db.documentdb_client import DocumentDBClientOptions, DocumentDBClient
from dpypelines.pipeline.db.datasets_service import DatasetsService
from dpypelines.pipeline.db.db_collection_factories import DatasetsServiceFactory
import dpypelines.pipeline.db.db_models as models
from dpypelines.pipeline.db.db_model_factories import (
    DatasetEventFactory,
    DatasetEventDataFactory,
)
from dpypelines.pipeline.process_zip_file import S3Object


def test_datasets_service(
    mock_datasets_collection,
    mock_dataset_db_collection,
    mock_statuses_collection,
    mock_status_db_collection,
):
    mock_datasets_service = DatasetsService(
        mock_datasets_collection, mock_statuses_collection
    )
    assert mock_datasets_service.datasets_collection == mock_datasets_collection
    assert mock_datasets_service.statuses_collection == mock_statuses_collection


@patch("dpytools.db.documentdb_client.MongoClient")
def test_create_dataset_service_factory(mock_MongoClient):
    """
    Test that the `DocumentDBClient.connect` method returns a `MongoClient` object.
    """
    client_options = DocumentDBClientOptions(host="localhost", port="27017")
    client = DocumentDBClient(client_options, "test-db")
    mock_mongo_client = MagicMock(name="MongoClient")
    mock_MongoClient.return_value = mock_mongo_client
    client.connect()
    datasets_service = DatasetsServiceFactory.create_db_datasets_service(client=client)
    assert datasets_service.datasets_collection.collection_name == "datasets"
    assert datasets_service.statuses_collection.collection_name == "dataset_statuses"


def test_create_new_dataset(mock_datasets_service_unit):
    """
    Test that the create_new_dataset() method updates both the datasets and statuses collections with the correct values.
    """
    new_dataset, status_oid = mock_datasets_service_unit.create_new_dataset(
        dataset_id="dataset_id",
        s3_object_key="input/dataset_id.zip",
        filename="datset_id.zip",
        additional_data={"key": "value"},
        latest_edition_id="edition_id",
        latest_version_id=1,
    )
    status = new_dataset.statuses[str(status_oid)]

    assert isinstance(new_dataset, models.Dataset)
    assert status.dataset_id == "dataset_id"
    assert status.edition_id == "edition_id"
    assert status.version_id == 1
    assert status.events[0].event_type == models.DatasetEventType.RECEIVED
    assert status.events[0].event_data.s3_object_key == "input/dataset_id.zip"
    assert status.events[0].event_data.additional_data == {"key": "value"}


def test_update_dataset_existing_status(mock_datasets_service_unit):
    """
    Test that the update_dataset_existing_status() method updates both the datasets and statuses collections with the correct values.
    """
    dataset = mock_datasets_service_unit.datasets_collection._DatasetsCollection__collection.test_datasets[
        0
    ]

    status_oid = list(dataset["statuses"].keys())[0]
    event = DatasetEventFactory.create_processing_dataset_event(
        dataset_id=dataset["dataset_id"],
        event_data=DatasetEventDataFactory.create_dataset_event_data(
            s3_object_key="input/dataset_id_1.zip"
        ),
    )
    updated_dataset = mock_datasets_service_unit.update_dataset_existing_status(
        dataset["dataset_id"], status_oid, event, models.DatasetStatusType.PROCESSING
    )
    updated_status = updated_dataset.statuses[status_oid]
    assert updated_status.status == models.DatasetStatusType.PROCESSING
    assert len(updated_status.events) == 2
    assert updated_status.events[1].event_type == models.DatasetEventType.PROCESSING
    assert updated_dataset.updated_at == updated_status.events[1].timestamp


def test_update_dataset_new_status(mock_datasets_service_unit):
    """
    Test that the update_dataset_new_status() method updates both the datasets and statuses collections with the correct values.
    """
    dataset = mock_datasets_service_unit.datasets_collection._DatasetsCollection__collection.test_datasets[
        0
    ]

    new_status = mock_datasets_service_unit.statuses_collection.create_new_status(
        s3_object_key="input/dataset_id_1.zip",
        dataset_id=dataset["dataset_id"],
        filename="dataset_id_1.zip",
        edition_id="edition_id",
        version_id=2,
        additional_data={"key": "value"},
    )

    updated_dataset = mock_datasets_service_unit.update_dataset_new_status(
        dataset["dataset_id"], new_status
    )
    new_status_id = str(new_status.id)
    assert updated_dataset.statuses[new_status_id] == new_status


def test_create_dataset_exists(mock_datasets_service_unit):
    s3_object = S3Object("bucket/input/dataset_id_1 - test.zip")
    updated_dataset, status_oid = (
        mock_datasets_service_unit.create_dataset_if_not_exists(s3_object=s3_object)
    )
    new_status = updated_dataset.statuses[str(status_oid)]
    assert len(updated_dataset.statuses) == 2
    assert updated_dataset.updated_at == new_status.events[0].timestamp


def test_create_dataset_not_exists(mock_datasets_service_unit):
    s3_object = S3Object("bucket/input/dataset_id_5.zip")
    updated_dataset, status_oid = (
        mock_datasets_service_unit.create_dataset_if_not_exists(s3_object=s3_object)
    )
    new_status = updated_dataset.statuses[str(status_oid)]
    assert len(updated_dataset.statuses) == 1
    assert updated_dataset.updated_at == new_status.events[0].timestamp
