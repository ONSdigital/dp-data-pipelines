import dpypelines.pipeline.db.db_models as models

from dpypelines.pipeline.db.db_model_factories import (
    DatasetEventDataFactory,
    DatasetEventFactory,
)


def test_dataset_statuses_collection(mock_statuses_collection):
    """
    Test that a statuses collection can be created.
    """
    mock_status_db_collection = mock_statuses_collection.initialise_collection()

    assert (
        mock_statuses_collection._DatasetStatusesCollection__collection
        == mock_status_db_collection
    )
    mock_statuses_collection.client.get_collection.assert_called_once_with(
        "dataset_statuses"
    )
    assert mock_statuses_collection.collection_name == "dataset_statuses"


def test_create_new_dataset_status(mock_statuses_collection, mock_status_db_collection):
    """
    Test that a new status can be added to the statuses collection.
    """
    new_status = mock_statuses_collection.create_new_status(
        s3_object_key="input/dataset_id.zip",
        filename="dataset_id.zip",
        dataset_id="dataset_id",
    )
    events = new_status.events
    assert isinstance(new_status, models.DatasetStatus)
    assert new_status.status.value == "PENDING"
    assert new_status.file_name == "dataset_id.zip"
    assert len(events) == 1
    assert events[0].event_type.value == "RECEIVED"


def test_update_dataset_status(mock_statuses_collection, mock_status_db_collection):
    """
    Test that a status can be updated.
    """
    status_id_to_update = mock_statuses_collection.get_status("dataset_id_1").id

    event = DatasetEventFactory.create_processing_dataset_event(
        dataset_id="dataset_id_1",
        event_data=DatasetEventDataFactory.create_dataset_event_data(
            s3_object_key="input/dataset_id_1.zip"
        ),
    )

    updated_status = mock_statuses_collection.update_status(
        status_oid=status_id_to_update,
        event=event,
        new_status=models.DatasetStatusType.PROCESSING,
    )

    assert updated_status.status.value == "PROCESSING"
    assert len(updated_status.events) == 2
    assert updated_status.events[1].event_type.value == "PROCESSING"


def test_get_all_dataset_statuses(mock_statuses_collection, mock_status_db_collection):
    """
    Test that the get_all_statuses_for_dataset_id() method returns the correct values.
    """
    all_statuses = mock_statuses_collection.get_all_statuses_for_dataset_id(
        "dataset_id_1"
    )
    matching_status = all_statuses[0]
    assert len(all_statuses) == 1
    assert isinstance(matching_status, models.DatasetStatus)
    assert matching_status.dataset_id == "dataset_id_1"


def test_get_dataset_status(mock_statuses_collection, mock_status_db_collection):
    """
    Test that the get_status() method returns the correct values.
    """
    all_statuses = mock_statuses_collection.get_all_statuses_for_dataset_id(
        "dataset_id_1"
    )
    id_of_status_to_get = all_statuses[0].id
    status = mock_statuses_collection.get_status(id_of_status_to_get)

    assert isinstance(status, models.DatasetStatus)
    assert status.id == id_of_status_to_get
    assert status.dataset_id == "dataset_id_1"


def test_handle_dataset_api_upload_event(mock_statuses_collection):
    """
    Test that the _handle_upload_event() method correctly handles events that upload to the Dataset API.
    """
    event = DatasetEventFactory.create_uploaded_dataset_event(
        "dataset_id",
        DatasetEventDataFactory.create_dataset_event_data(
            s3_object_key="input/dataset_id.zip",
            upload_location=models.UploadLocation.DATASET_API,
        ),
    )
    update_values = mock_statuses_collection._handle_upload_event(event, {})
    assert update_values["uploaded_to_dataset_api"]


def test_handle_upload_service_upload_event(mock_statuses_collection):
    """
    Test that the _handle_upload_event() method correctly handles events that upload to the Upload Service.
    """
    event = DatasetEventFactory.create_uploaded_dataset_event(
        "dataset_id",
        DatasetEventDataFactory.create_dataset_event_data(
            s3_object_key="input/dataset_id.zip",
            upload_location=models.UploadLocation.UPLOAD_SERVICE,
        ),
    )
    update_values = mock_statuses_collection._handle_upload_event(event, {})
    assert update_values["uploaded_to_upload_service"]
