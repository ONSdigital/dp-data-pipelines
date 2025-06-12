from typing import Any, Dict, List
import mongomock
from testcontainers.mongodb import MongoDbContainer
from datetime import datetime as dt


class MockDBOperations:
    def __init__(self, mongo: MongoDbContainer):
        self.mongo = mongo
        self.connection_url = mongo.get_connection_url()
        self.client = mongo.get_connection_client()
        self.database = self.client.get_database(mongo.dbname)
        self.datasets_collection = self.database.get_collection("datasets")
        self.statuses_collection = self.database.get_collection("dataset_statuses")
        self.insert_data()

    def insert_data(self):
        self.datasets = generate_test_datasets()
        self.datasets_collection.insert_many(self.datasets)
        for dataset in self.datasets:
            for id, status in dataset["statuses"].items():
                self.statuses_collection.insert_one(status)

    def delete_data(self):
        self.datasets_collection.delete_many({})
        self.statuses_collection.delete_many({})


def create_event(
    dataset_id: str,
    event_type: str,
) -> Dict[str, Any]:
    """
    Create a test event.
    """
    event_oid = mongomock.ObjectId()
    err_msg = "Pipeline failed" if event_type == "FAILED" else None
    upload_location = "DATASET_API" if event_type == "UPLOADED" else None
    return {
        "_id": event_oid,
        "dataset_id": dataset_id,
        "error_message": err_msg,
        "event_data": {
            "additional_data": None,
            "s3_object_key": f"input/{dataset_id}.zip",
            "upload_location": upload_location,
        },
        "event_type": event_type,
        "id": str(event_oid),
        "last_retry_timestamp": None,
        "retry_count": 0,
        "timestamp": dt.now().isoformat(),
    }


def generate_test_statuses() -> List[Dict[str, Any]]:
    """
    Generate test statuses.
    """
    # The first item in the list of values for each dataset_id is the status type, and the remaining values are the event types
    status_events = {
        "dataset_id_1": ["PENDING", "RECEIVED"],
        "dataset_id_2": ["PROCESSING", "RECEIVED", "PROCESSING", "UPLOADED"],
        "dataset_id_3": ["FAILED", "RECEIVED", "PROCESSING", "FAILED"],
        "dataset_id_4": [
            "COMPLETED",
            "RECEIVED",
            "PROCESSING",
            "UPLOADED",
            "COMPLETED",
        ],
    }
    statuses = []
    for dataset_id, events in status_events.items():
        status_oid = mongomock.ObjectId()
        created_at = dt.now().isoformat()
        status_dict = {
            "_id": status_oid,
            "created_at": created_at,
            "dataset_id": dataset_id,
            "edition_id": f"edition_id_for_{dataset_id}",
            "events": [
                create_event(
                    dataset_id,
                    event,
                )
                for event in events[1:]
            ],
            "file_name": f"{dataset_id}.zip",
            "id": str(status_oid),
            "last_retry_timestamp": None,
            "retry_count": 0,
            "status": events[0],
            "uploaded_to_dataset_api": False,
            "uploaded_to_upload_service": False,
            "version_id": 1,
        }
        status_dict["updated_at"] = status_dict["events"][-1]["timestamp"]
        status_dict["error_message"] = status_dict["events"][-1]["error_message"]
        for event_dict in status_dict["events"]:
            if event_dict["event_type"] in ("UPLOADED", "COMPLETED"):
                status_dict["uploaded_to_dataset_api"] = True
        statuses.append(status_dict)
    return statuses


def generate_test_datasets() -> List[Dict[str, Any]]:
    """
    Generate test datasets.
    """
    dataset_ids = ["dataset_id_1", "dataset_id_2", "dataset_id_3", "dataset_id_4"]
    statuses = generate_test_statuses()
    datasets = []
    for idx, dataset_id in enumerate(dataset_ids):
        dataset_oid = mongomock.ObjectId()
        created_at = dt.now().isoformat()
        edition_id = f"edition_id_for_{dataset_id}"
        version_id = 1
        dataset = {
            "_id": dataset_oid,
            "id": str(dataset_oid),
            "dataset_id": dataset_id,
            "created_at": created_at,
            "updated_at": created_at,
            "latest_edition_id": edition_id,
            "latest_version_id": version_id,
            "statuses": {statuses[idx]["id"]: statuses[idx]},
            # f"statuses.{statuses[idx]['id']}": statuses[idx],
        }
        datasets.append(dataset)
    return datasets
