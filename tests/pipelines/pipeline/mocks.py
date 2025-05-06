from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import mongomock
from datetime import datetime as dt


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
    event_type: str,
):
    event_oid = mongomock.ObjectId()
    err_msg = "Pipeline failed" if event_type == "FAILED" else None
    upload_location = "dataset_api" if event_type == "UPLOADED" else None
    return {
        "_id": event_oid,
        "dataset_id": "dataset_id",
        "error_message": err_msg,
        "event_data": {
            "additional_data": None,
            "s3_object_key": "input/dataset_id.zip",
            "upload_location": upload_location,
        },
        "event_type": event_type,
        "id": str(event_oid),
        "last_retry_timestamp": None,
        "retry_count": 0,
        "timestamp": dt.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


def generate_test_statuses():
    status_events = {
        "PENDING": ["RECEIVED"],
        "PROCESSING": ["RECEIVED", "PROCESSING", "UPLOADED"],
        "FAILED": ["RECEIVED", "PROCESSING", "FAILED"],
        "COMPLETED": ["RECEIVED", "PROCESSING", "UPLOADED", "COMPLETED"],
    }
    statuses = []
    for status, events in status_events.items():
        status_oid = mongomock.ObjectId()
        created_at = dt.now().strftime("%Y-%m-%dT%H:%M:%S")
        status_dict = {
            "_id": status_oid,
            "created_at": created_at,
            "dataset_id": "dataset_id",
            "edition_id": None,
            "events": [
                create_event(
                    event,
                )
                for event in events
            ],
            "file_name": "dataset_id.zip",
            "id": str(status_oid),
            "last_retry_timestamp": None,
            "retry_count": 0,
            "status": status,
            "uploaded_to_dataset_api": False,
            "uploaded_to_upload_service": False,
            "version_id": None,
        }
        status_dict["updated_at"] = status_dict["events"][-1]["timestamp"]
        status_dict["error_message"] = status_dict["events"][-1]["error_message"]
        for event_dict in status_dict["events"]:
            if event_dict["event_type"] in ("UPLOADED", "COMPLETED"):
                status_dict["uploaded_to_dataset_api"] = True
        statuses.append(status_dict)
    return statuses


def get_matching_element(
    dict_list: List[Dict[str, Any]], filter_by: Dict[str, Any]
) -> Tuple[int, Optional[Dict[str, Any]]]:
    for index, status in enumerate(dict_list):
        for k, v in filter_by.items():
            if k in status.keys() and status[k] == v:
                return (index, status)
            else:
                continue
        # return (index, status)
    return (-1, None)


def update_test_status(
    test_statuses: List[Dict[str, Any]],
    filter_by: Dict[str, Any],
    update_values: Dict[str, Any],
):
    matching_element = get_matching_element(test_statuses, filter_by)
    if matching_element[1] is None:
        raise Exception(f"Could not find matching test status for {filter_by}")

    for key in update_values:
        matching_element[1][key] = update_values[key]

    test_statuses[matching_element[0]] = matching_element[1]
    return matching_element[1]


class MockDBCollection:
    def __init__(self, collection: mongomock.Collection):
        self.collection = collection
        self.test_statuses = generate_test_statuses()

    def create_one_document(self, status_dict: Dict) -> Dict:
        return status_dict

    def read_one_document(self, filter_by: Dict[str, Any]) -> Dict[str, Any]:
        matching_element = get_matching_element(self.test_statuses, filter_by)
        return matching_element[1]

    def update_one_document(
        self, filter_by: Dict[str, Any], update_values: Dict[str, Any]
    ) -> Dict[str, Any]:
        return update_test_status(self.test_statuses, filter_by, update_values)

    def read_many_documents(self, filter_by: Dict) -> List[Dict]:
        return self.test_statuses
