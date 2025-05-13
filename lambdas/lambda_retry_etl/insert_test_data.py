from pymongo import MongoClient
import dpypelines.pipeline.models.db_models as models
from datetime import datetime as dt
from random import randint


def insert_test_data(client: MongoClient):
    # Set up client
    # client = MongoClient("localhost", 27017, uuidRepresentation="standard")

    # Create database
    db = client.state

    # Create collections
    datasets = db.datasets
    dataset_statuses = db.dataset_statuses
    # dataset_events = db.dataset_events

    # Delete existing documents
    datasets.delete_many({})
    dataset_statuses.delete_many({})
    # dataset_events.delete_many({})

    # Create test `dataset` documents and insert into `datasets` collection
    test_datasets = [
        models.Dataset(
            dataset_id=f"dataset_id_{i}",
            latest_edition_id=f"edition_id_for_dataset_id_{i}",
            latest_version_id=randint(0, 9),
            created_at=dt.now().isoformat(),
            statuses={},
        ).model_dump()
        for i in range(10)
    ]

    datasets.insert_many(test_datasets)

    # Create test `dataset_status` documents and insert into `dataset_statuses` collection
    test_dataset_statuses = [
        models.DatasetStatus(
            dataset_id=f"dataset_id_{i}",
            created_at=dt.now().isoformat(),
            updated_at=dt.now().isoformat(),
            file_name=f"dataset_id_{i}.zip",
        ).model_dump()
        for i in range(10)
    ]
    dataset_statuses.insert_many(test_dataset_statuses)

    return db, datasets, dataset_statuses


statuses = {
    "status_id_1": {
        "status": models.DatasetStatusType,
        "updated_at": dt.now().isoformat(),
    }
}
