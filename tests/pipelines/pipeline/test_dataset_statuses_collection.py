import mongomock
import pytest
from dpypelines.pipeline.db.db_collections import DatasetStatusesCollection
import dpypelines.pipeline.models.db_models as models
from tests.pipelines.pipeline.mocks import (
    MockDBCollection,
)


@pytest.fixture
def mock_statuses_collection():
    mock_collection = mongomock.MongoClient().state.dataset_statuses
    mock_db_collection = MockDBCollection(mock_collection)
    return DatasetStatusesCollection(mock_db_collection, "dataset_id")


def test_dataset_statuses_collection():
    mock_collection = mongomock.MongoClient().state.dataset_statuses

    mock_db_collection = MockDBCollection(mock_collection)
    statuses_collection = DatasetStatusesCollection(mock_db_collection, "dataset_id")
    assert (
        statuses_collection._DatasetStatusesCollection__collection == mock_db_collection
    )
    assert statuses_collection.dataset_id == "dataset_id"


def test_create_new_dataset_status(mock_statuses_collection):
    new_status = mock_statuses_collection.create_new_status("input/dataset_id.zip")

    assert isinstance(new_status, models.DatasetStatus)
    assert new_status.status.value == "PENDING"
    assert len(new_status.events) == 1
    assert new_status.events[0].event_type.value == "RECEIVED"


def test_update_dataset_status(mock_statuses_collection):
    all_statuses = mock_statuses_collection.get_all_statuses_for_dataset_id()
    id_of_status_to_update = all_statuses[0].id

    updated_status = mock_statuses_collection.update_status(
        status_oid=id_of_status_to_update,
        event_type=models.DatasetEventType.PROCESSING,
        new_status=models.DatasetStatusType.PROCESSING,
        s3_object_key="input/dataset_id.zip",
    )

    assert updated_status.status.value == "PROCESSING"
    assert len(updated_status.events) == 2
    assert updated_status.events[1].event_type.value == "PROCESSING"


def test_get_all_dataset_statuses(mock_statuses_collection):
    all_statuses = mock_statuses_collection.get_all_statuses_for_dataset_id()

    assert len(all_statuses) == 4


def test_get_dataset_status(mock_statuses_collection):
    all_statuses = mock_statuses_collection.get_all_statuses_for_dataset_id()
    id_of_status_to_get = all_statuses[0].id
    status = mock_statuses_collection.get_status(id_of_status_to_get)

    assert isinstance(status, models.DatasetStatus)
    assert status.id == id_of_status_to_get
