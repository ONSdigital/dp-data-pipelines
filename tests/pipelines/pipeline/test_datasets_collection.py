import dpypelines.pipeline.db.db_models as models


def test_dataset_collection(mock_datasets_collection):
    """
    Test that a datasets collection can be created.
    """
    mock_datasets_db_collection = mock_datasets_collection.initialise_collection()

    assert (
        mock_datasets_collection._DatasetsCollection__collection
        == mock_datasets_db_collection
    )
    mock_datasets_collection.client.get_collection.assert_called_once_with("datasets")
    assert mock_datasets_collection.collection_name == "datasets"


def test_get_all_datasets(mock_datasets_collection, mock_dataset_db_collection):
    """
    Test that the get_all_datasets() method returns the correct values.
    """
    all_datasets = mock_datasets_collection.get_all_datasets()

    assert len(all_datasets) == 4
    for dataset in all_datasets:
        assert isinstance(dataset, models.Dataset)


def test_get_dataset(mock_datasets_collection, mock_dataset_db_collection):
    """
    Test that the get_dataset() method returns the correct values.
    """
    dataset = mock_datasets_collection.get_dataset("dataset_id_1")
    status = list(dataset.statuses.items())[0][1]

    assert dataset.dataset_id == "dataset_id_1"
    assert dataset.latest_edition_id == "edition_id_for_dataset_id_1"
    assert status.status == models.DatasetStatusType.PENDING
    assert len(status.events) == 1
    assert status.events[0].event_type == models.DatasetEventType.RECEIVED


def test_dataset_exists(mock_datasets_collection, mock_dataset_db_collection):
    assert mock_datasets_collection.dataset_exists("dataset_id_1")
    assert not mock_datasets_collection.dataset_exists("not_a_dataset")
