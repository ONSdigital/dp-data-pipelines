from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId
from dpytools.db.documentdb_client import DocumentDBClient

from dpypelines.pipeline.db.base_collection import BaseCollection
import dpypelines.pipeline.db.db_models as models
from dpypelines.pipeline.db.db_model_factories import (
    DatasetStatusFactory,
    DatasetEventFactory,
    DatasetEventDataFactory,
)
from dpypelines.pipeline.db.db_utils import (
    _convert_event_ids_and_validate,
)
from dpypelines.pipeline.errors import (
    DocumentNotFoundException,
    DocumentNotCreatedException,
    DocumentNotUpdatedException,
)


class DatasetStatusesCollection(BaseCollection):
    """
    Class to manage interactions with the statuses collection in the state management database.
    """

    def __init__(self, client: DocumentDBClient):
        super().__init__(client, "dataset_statuses")

    def initialise_collection(self):
        self.__collection = super().initialise_collection()
        return self.__collection

    def get_status(self, status_oid: ObjectId) -> models.DatasetStatus:
        """
        Get status from statuses collection and convert to DatasetStatus model.

        :param status_oid: The ObjectID of the status document to retrieve

        :return: models.DatasetStatus
        """
        # Get status document for the given ObjectID
        status_document = self.__collection.read_one_document({"_id": status_oid})

        if not status_document:
            raise DocumentNotFoundException(
                message="Status document not found in collection",
                query_filter={"_id": status_oid},
            )

        # Convert event ID values to ObjectIds and generate model
        status_model = _convert_event_ids_and_validate(status_document=status_document)

        return status_model

    def get_all_statuses_for_dataset_id(
        self, dataset_id: str
    ) -> List[models.DatasetStatus]:
        """
        Get all statuses associated with the given dataset_id and convert to DatasetStatus models.

        :param dataset_id: The dataset ID to retrieve all statuses for.

        :return: List[models.DatasetStatus]
        """
        # Get all status documents for the given dataset_id
        all_status_documents = self.__collection.read_many_documents(
            {"dataset_id": dataset_id}
        )

        if not all_status_documents:
            raise DocumentNotFoundException(
                message="No status documents found for dataset_id",
                query_filter={"dataset_id": dataset_id},
            )

        # Convert event ID values to ObjectIds and generate models
        all_status_models = [
            _convert_event_ids_and_validate(document)
            for document in all_status_documents
        ]

        return all_status_models

    def create_new_status(
        self,
        dataset_id: str,
        s3_object_key: str,
        filename: str,
        edition_id: Optional[str] = None,
        version_id: Optional[int] = None,
        additional_data: Optional[Dict[str, Any]] = None,
    ) -> models.DatasetStatus:
        """
        Create a new status document in the statuses collection.

        :param s3_object_key: The object key of the file received in the S3 ingest bucket
        :param dataset_id: The dataset ID of the dataset associated with the new status
        :param edition_id: The edition ID of the relevant dataset
        :param version_id: The version ID of the relevant edition
        :param additional_data: Any additional data associated with the status event

        :return: models.DatasetStatus
        """
        # Create a DatasetEvent model with event_type of "RECEIVED"
        event_model = DatasetEventFactory.create_received_dataset_event(
            dataset_id=dataset_id,
            event_data=DatasetEventDataFactory.create_dataset_event_data(
                s3_object_key=s3_object_key, additional_data=additional_data
            ),
        )

        # Create a DatasetStatus model with status_type of "PENDING"
        status_model = DatasetStatusFactory.create_dataset_status(
            dataset_id=dataset_id,
            created_at=event_model.timestamp,
            updated_at=event_model.timestamp,
            file_name=filename,
            edition_id=edition_id,
            version_id=version_id,
            events=[event_model],
        )

        status_dict = status_model.dict_for_mongodb()
        status_dict["_id"] = ObjectId(status_dict["id"])

        # Create new status document in statuses collection
        result = self.__collection.create_one_document(status_dict)

        if not result.acknowledged:
            raise DocumentNotCreatedException(
                message="New status document not created in collection",
                data={"dataset_id": status_model.dataset_id},
            )

        return status_model

    def update_status(
        self,
        status_oid: ObjectId,
        event: models.DatasetEvent,
        new_status: models.DatasetStatusType,
    ) -> models.DatasetStatus:
        """
        Update status details when new events occur during ETL pipeline processing.

        :param status_oid: The ObjectId of the status to be updated
        :param event: The event associated with the status update
        :param new_status: The status type associated with the update event
        :param upload_event: Whether the update event is related to uploading to an external service (DatasetAPI or Upload Service)

        :return: models.DatasetStatus
        """
        # Get current status
        current_status_model = self.get_status(status_oid)

        # Append new event to current_status.events
        current_status_model.events.append(event)

        # Generate `update_values` dict to pass to `update_one_document` method
        update_values = {
            "updated_at": event.timestamp.isoformat(),
            "status": new_status.name,
            "events": [
                event.dict_for_mongodb() for event in current_status_model.events
            ],
            "error_message": event.error_message,
            "retry_count": event.retry_count,
            "last_retry_timestamp": event.last_retry_timestamp,
        }

        # Handle upload events
        if event.is_upload_event():
            update_values = self._handle_upload_event(event, update_values)

        # Update status document with new values
        result = self.__collection.update_one_document(
            {"_id": status_oid}, update_values
        )

        if not result.acknowledged:
            raise DocumentNotUpdatedException(
                message="Status document not updated in collection",
                data={"_id": status_oid},
            )

        # Get updated status document and convert to DatasetStatus model
        updated_status_model = self.get_status(status_oid)

        return updated_status_model

    def _handle_upload_event(
        self, event: models.DatasetEvent, update_values: Dict[str, Any]
    ) -> Dict[str, Any]:
        match event.event_data.upload_location:  # type:ignore
            case models.UploadLocation.DATASET_API:
                update_values["uploaded_to_dataset_api"] = True
            case models.UploadLocation.UPLOAD_SERVICE:
                update_values["uploaded_to_upload_service"] = True
        return update_values
