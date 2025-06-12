from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import mongomock
from datetime import datetime as dt

from pymongo.results import InsertOneResult, UpdateResult

import dpypelines.pipeline.db.db_models as models


class MockLocalDirectoryStore:
    calls = []

    def __init__(self, local_dir: Path, expected_path: str, files_to_return: list[str]):
        self.local_dir = local_dir
        self.expected_path = expected_path
        self.files_to_return = files_to_return

    def get_file_names(self):
        str_local_dir = str(self.local_dir)
        self.calls.append(str_local_dir)
        if str_local_dir == self.expected_path:
            return self.files_to_return

        return []


def mock_path_constructor(
    path_arg: str, r_glob_return_value: list, path_instances: dict
):
    mock_instance = MagicMock(name=f"Path({path_arg})")
    path_instances[path_arg] = mock_instance

    mock_instance.rglob.return_value = r_glob_return_value

    return mock_instance


def mock_decompress_zip_file(zip_path: str, expected_path: str):
    if zip_path != expected_path:
        raise Exception(f"Expected {expected_path} but received {zip_path}")

    path = Path(zip_path)
    return path.stem


def create_event(
    dataset_id: str,
    event_type: str,
) -> Dict[str, Any]:
    """
    Create a test event.
    """
    event_oid = mongomock.ObjectId()
    err_msg = "Pipeline failed" if event_type == "FAILED" else None
    upload_location = (
        models.UploadLocation.DATASET_API if event_type == "UPLOADED" else None
    )
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
            f"statuses.{statuses[idx]['id']}": statuses[idx],
        }
        datasets.append(dataset)
    return datasets


def get_matching_dataset_element(
    dict_list: List[Dict[str, Any]], filter_by: Dict[str, Any]
) -> Tuple[int, Optional[Dict[str, Any]]]:
    for index, status in enumerate(dict_list):
        for k, v in filter_by.items():
            if k in status.keys() and status[k] == v:
                return (index, status)
            else:
                continue
    return (-1, None)


def get_matching_status_element(
    dict_list: List[Dict[str, Any]], filter_by: Dict[str, Any]
) -> Tuple[int, Optional[Dict[str, Any]]]:
    for index, status in enumerate(dict_list):
        for k, v in filter_by.items():
            if k in status.keys() and status[k] == v:
                return (index, status)
            else:
                continue
        return (index, status)
    return (-1, None)


def update_test_status(
    test_statuses: List[Dict[str, Any]],
    filter_by: Dict[str, Any],
    update_values: Dict[str, Any],
):
    """
    Update a test status.
    """
    matching_element = get_matching_status_element(test_statuses, filter_by)
    if matching_element[1] is None:
        raise Exception(f"Could not find matching test status for {filter_by}")

    for key in update_values:
        matching_element[1][key] = update_values[key]

    test_statuses[matching_element[0]] = matching_element[1]
    return test_statuses


def update_test_dataset(
    test_datasets: List[Dict[str, Any]],
    filter_by: Dict[str, Any],
    update_values: Dict[str, Any],
):
    """
    Update a test dataset.
    """
    matching_element = get_matching_dataset_element(test_datasets, filter_by)
    if matching_element[1] is None:
        raise Exception(f"Could not find matching test dataset for {filter_by}")
    element_to_update = matching_element[1]
    for key in update_values:
        is_dictionary = "." in key
        if is_dictionary:
            key_split = key.split(".")
            dictionary_key = key_split[0]
            dictionary_item = key_split[1]
            element_to_update[dictionary_key][dictionary_item] = update_values[key]
        element_to_update[key] = update_values[key]
    test_datasets[matching_element[0]] = matching_element[1]
    return test_datasets


class MockStatusDBCollection:
    def __init__(self, collection: mongomock.Collection):
        self.collection = collection
        self.test_statuses = generate_test_statuses()

    def create_one_document(self, status_dict: Dict[str, Any]) -> InsertOneResult:
        return InsertOneResult(status_dict["id"], True)

    def read_one_document(self, filter_by: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        matching_element = get_matching_status_element(self.test_statuses, filter_by)  # type:ignore
        return matching_element[1]

    def update_one_document(
        self, filter_by: Dict[str, Any], update_values: Dict[str, Any]
    ) -> UpdateResult:
        self.test_statuses = update_test_status(
            self.test_statuses, filter_by, update_values
        )  # type:ignore
        return UpdateResult(raw_result={}, acknowledged=True)

    def read_many_documents(
        self, filter_by: Dict[str, Any]
    ) -> Optional[List[Dict[str, Any]]]:
        matching_documents = [
            status
            for status in self.test_statuses
            for k, v in filter_by.items()
            if status[k] == v
        ]
        return matching_documents


class MockDatasetDBCollection:
    def __init__(self, collection: mongomock.Collection):
        self.collection = collection
        self.test_datasets = generate_test_datasets()

    def create_one_document(self, dataset_dict: Dict[str, Any]) -> InsertOneResult:
        return InsertOneResult(inserted_id=dataset_dict["id"], acknowledged=True)

    def read_one_document(self, filter_by: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        matching_element = get_matching_dataset_element(self.test_datasets, filter_by)
        return matching_element[1]

    def read_many_documents(self) -> List[Dict[str, Any]]:
        return self.test_datasets

    def update_one_document(
        self, filter_by: Dict[str, Any], update_values: Dict[str, Any]
    ) -> UpdateResult:
        self.test_datasets = update_test_dataset(
            self.test_datasets, filter_by, update_values
        )
        return UpdateResult(raw_result={}, acknowledged=True)
