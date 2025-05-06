from datetime import datetime as dt
from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId
from dpytools.db.documentdb_client import DBCollection

import dpypelines.pipeline.models.db_models as models


class DatasetStatusesCollection:
    def __init__(self, collection: DBCollection, dataset_id: str):
        self.__collection = collection
        self.dataset_id = dataset_id

    def get_status(self, status_oid: ObjectId) -> models.DatasetStatus:
        """
        Get status from `dataset_statuses` collection and convert to DatasetStatus model.

        :param status_oid: The ObjectID of the status document to retrieve

        :return: models.DatasetStatus
        """
        # Get status document for the given ObjectID
        status_document = self.__collection.read_one_document({"_id": status_oid})

        # Convert event id values to ObjectIds
        for event in status_document["events"]:
            event["_id"] = ObjectId(event["id"])

        # Convert result into DatasetStatus model
        status_model = models.DatasetStatus.model_validate(status_document)
        return status_model

    def get_all_statuses_for_dataset_id(self) -> List[models.DatasetStatus]:
        """
        Get all statuses associated with the given `dataset_id`.

        :return: List[models.DatasetStatus]
        """
        # Get all status document for the given dataset_id
        all_status_documents = self.__collection.read_many_documents(
            {"dataset_id": self.dataset_id}
        )

        # Convert results into a list of DatasetStatus models
        all_status_models = [
            self.get_status(status_document["_id"])
            for status_document in all_status_documents
        ]
        return all_status_models

    def create_new_status(
        self, s3_object_key: str, additional_data: Optional[Dict[str, Any]] = {}
    ) -> models.DatasetStatus:
        """
        Create a new status document in the `dataset_statuses` collection when a new file submission arrives in the S3 ingest bucket.

        :param s3_object_key: The object key of the file received in the S3 ingest bucket
        :param additional_data: Any additional data associated with the event

        :return: models.DatasetStatus
        """

        # Create a `DatasetStatus` model with `status_type` of "PENDING"
        timestamp = dt.now()
        status_model = models.DatasetStatus(
            _id=ObjectId(),
            dataset_id=self.dataset_id,
            created_at=timestamp,
            updated_at=timestamp,
            file_name=f"{self.dataset_id}.zip",
            status_type=models.DatasetStatusType.PENDING,
        )

        # Create a `DatasetEvent` model with `event_type` "RECEIVED"
        event_model = models.DatasetEvent(
            _id=ObjectId(),
            dataset_id=self.dataset_id,
            timestamp=status_model.created_at,
            event_type=models.DatasetEventType.RECEIVED,
            event_data=models.DatasetEventData(
                s3_object_key=s3_object_key, additional_data=additional_data
            ),
            retry_count=status_model.retry_count,
        )
        status_model.events.append(event_model)

        # Create new status document in `dataset_statuses` collection
        status_dict = status_model.dict_for_mongodb()
        status_dict["_id"] = ObjectId(status_dict["id"])
        self.__collection.create_one_document(status_dict)

        return status_model

    def update_status(
        self,
        status_oid: ObjectId,
        event_type: models.DatasetEventType,
        new_status: models.DatasetStatusType,
        s3_object_key: str,
        upload_event: Optional[bool] = None,
        upload_location: Optional[str] = None,
        additional_data: Optional[Dict[str, Any]] = None,
        error_msg: Optional[str] = None,
        retry_count: Optional[int] = 0,
        last_retry_timestamp: Optional[dt] = None,
    ) -> models.DatasetStatus:
        """
        Update status details when new events occur during ETL pipeline processing.

        :param status_oid: The ObjectId of the status to be updated
        :param event_type: The event type associated with the update event
        :param new_status: The status type associated with the update event
        :param s3_object_key: The object key of the file received in the S3 ingest bucket
        :param upload_event: Whether the update event is related to uploading to an external service (DatasetAPI or Upload Service)
        :param upload_location: The location to which the data should be uploaded (DatasetAPI or Upload Service)
        :param additional_data: Any additional data associated with the update event
        :param error_msg: The message to be included if there is an error in processing
        :param retry_count: If this is a retry event, the count of the number of retries attempted
        :param last_retry_timestamp:If this is a retry event, the timestamp of the last retry event

        :return: models.DatasetStatus
        """
        # Get current status
        current_status_model = self.get_status(status_oid)

        # Create new event
        event_model = models.DatasetEvent(
            _id=ObjectId(),
            dataset_id=self.dataset_id,
            timestamp=dt.now(),
            event_type=event_type,
            event_data=models.DatasetEventData(
                s3_object_key=s3_object_key,
                upload_location=upload_location,
                additional_data=additional_data,
            ),
            error_message=error_msg,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

        # Append new event to current_status.events
        current_status_model.events.append(event_model)

        # Generate `update_values` dict to pass to `update_one_document` method
        update_values = {
            "updated_at": event_model.timestamp.isoformat(),
            "status": new_status.name,
            "events": [
                event.dict_for_mongodb() for event in current_status_model.events
            ],
            "error_message": error_msg,
            "retry_count": retry_count,
            "last_retry_timestamp": last_retry_timestamp,
        }

        # Handle upload events
        if upload_event and upload_location == "dataset_api":
            update_values["uploaded_to_dataset_api"] = True
        elif upload_event and upload_location == "upload_service":
            update_values["uploaded_to_upload_service"] = True

        # Update status document with new values
        self.__collection.update_one_document({"_id": status_oid}, update_values)

        # Get updated status document and convert to DatasetStatus model
        updated_status_model = self.get_status(status_oid)
        return updated_status_model
