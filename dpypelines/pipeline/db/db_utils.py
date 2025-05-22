from typing import Any, Dict
from bson.objectid import ObjectId

import dpypelines.pipeline.db.db_models as models


def _convert_status_ids_to_object_ids(
    dataset_document: Dict[str, Any],
) -> Dict[str, Any]:
    # Convert status IDs to ObjectIDs so that models can be validated
    for status_id, status in dataset_document["statuses"].items():
        status["_id"] = ObjectId(status_id)
        # Convert event IDs to ObjectIDs
        status = _convert_event_ids_to_object_ids(status)
    return dataset_document


def _convert_event_ids_to_object_ids(status_document: Dict[str, Any]) -> Dict[str, Any]:
    # Convert event IDs to ObjectIDs so that models can be validated
    for event in status_document["events"]:
        event["_id"] = ObjectId(event["id"])
    return status_document


def _convert_event_ids_and_validate(
    status_document: Dict[str, Any],
) -> models.DatasetStatus:
    """
    Convert event IDs to pymongo.ObjectIDs and return validated status model.
    """
    status_document = _convert_event_ids_to_object_ids(status_document)
    status_model = models.DatasetStatus.model_validate(status_document)
    return status_model


def _convert_status_ids_and_validate(
    dataset_document: Dict[str, Any],
) -> models.Dataset:
    """
    Convert status IDs to pymongo.ObjectIDs and return validated dataset model.
    """
    dataset_document = _convert_status_ids_to_object_ids(dataset_document)
    dataset_model = models.Dataset.model_validate(dataset_document)
    return dataset_model


def _get_statuses_key_with_dot_notation(status_oid: ObjectId):
    """
    To update an existing status in the datasets collection, append the status ID to `statuses` using dot notation.
    """
    return f"statuses.{str(status_oid)}"
