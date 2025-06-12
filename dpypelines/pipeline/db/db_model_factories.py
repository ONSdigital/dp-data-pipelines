from datetime import datetime
from bson.objectid import ObjectId

from typing import Any, Dict, List, Optional

import dpypelines.pipeline.db.db_models as models


class DatasetEventDataFactory:
    @staticmethod
    def create_dataset_event_data(
        s3_object_key: str,
        upload_location: Optional[models.UploadLocation] = None,
        additional_data: Optional[Dict[str, Any]] = None,
    ) -> models.DatasetEventData:
        """
        Create a new DatasetEventData model.

        :param s3_object_key: The S3 object key associated with the pipeline event
        :param upload_location: The upload location, if this is an upload event
        :param additional_data: Any additional data associated with the event

        :return: models.DatasetEventData
        """
        return models.DatasetEventData(
            s3_object_key=s3_object_key,
            upload_location=upload_location,
            additional_data=additional_data,
        )


class DatasetEventFactory:
    @staticmethod
    def create_dataset_event(
        dataset_id: str,
        event_type: models.DatasetEventType,
        timestamp: datetime = datetime.now(),
        event_data: Optional[models.DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetEvent:
        """
        Create a new DatasetEvent model.

        :param dataset_id: The dataset_id associated with the event
        :param event_type: The DatasetEventType associated with the event
        :param timestamp: The timestamp of the event
        :param event_data: The DatasetEventData associated with the event
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetEvent
        """
        return models.DatasetEvent(
            _id=ObjectId(),
            dataset_id=dataset_id,
            timestamp=timestamp,
            event_type=event_type,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_received_dataset_event(
        dataset_id: str,
        event_data: Optional[models.DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetEvent:
        """
        Create a new DatasetEvent model wih an event_type of "RECEIVED".

        :param dataset_id: The dataset_id associated with the event
        :param event_data: The DatasetEventData associated with the event
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetEvent
        """
        return DatasetEventFactory.create_dataset_event(
            dataset_id=dataset_id,
            event_type=models.DatasetEventType.RECEIVED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_processing_dataset_event(
        dataset_id: str,
        event_data: Optional[models.DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetEvent:
        """
        Create a new DatasetEvent model wih an event_type of "PROCESSING".

        :param dataset_id: The dataset_id associated with the event
        :param event_data: The DatasetEventData associated with the event
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetEvent
        """
        return DatasetEventFactory.create_dataset_event(
            dataset_id=dataset_id,
            event_type=models.DatasetEventType.PROCESSING,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_uploaded_dataset_event(
        dataset_id: str,
        event_data: Optional[models.DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetEvent:
        """
        Create a new DatasetEvent model wih an event_type of "UPLOADED".

        :param dataset_id: The dataset_id associated with the event
        :param event_data: The DatasetEventData associated with the event
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetEvent
        """
        return DatasetEventFactory.create_dataset_event(
            dataset_id=dataset_id,
            event_type=models.DatasetEventType.UPLOADED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_failed_dataset_event(
        dataset_id: str,
        event_data: Optional[models.DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetEvent:
        """
        Create a new DatasetEvent model wih an event_type of "FAILED".

        :param dataset_id: The dataset_id associated with the event
        :param event_data: The DatasetEventData associated with the event
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetEvent
        """
        return DatasetEventFactory.create_dataset_event(
            dataset_id=dataset_id,
            event_type=models.DatasetEventType.FAILED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_completed_dataset_event(
        dataset_id: str,
        event_data: Optional[models.DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetEvent:
        """
        Create a new DatasetEvent model wih an event_type of "COMPLETED".

        :param dataset_id: The dataset_id associated with the event
        :param event_data: The DatasetEventData associated with the event
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetEvent
        """
        return DatasetEventFactory.create_dataset_event(
            dataset_id=dataset_id,
            event_type=models.DatasetEventType.COMPLETED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )


class DatasetStatusFactory:
    @staticmethod
    def create_dataset_status(
        dataset_id: str,
        file_name: str,
        created_at: datetime = datetime.now(),
        updated_at: datetime = datetime.now(),
        status: models.DatasetStatusType = models.DatasetStatusType.PENDING,
        events: List[models.DatasetEvent] = [],
        edition_id: Optional[str] = None,
        version_id: Optional[int] = None,
        uploaded_to_dataset_api: bool = False,
        uploaded_to_upload_service: bool = False,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> models.DatasetStatus:
        """
        Create a new DatasetStatus model.

        :param dataset_id: The dataset_id associated with the new status
        :param file_name: The filename associated with the new status
        :param created_at: The timestamp of the creation of the new status
        :param updated_at: The timestamp of the last update to the status
        :param status: The status type of the new status
        :param events: A list of DatasetEvents associated with this status
        :param edition_id: The edition_id of the given dataset_id
        :param version_id: The version_id of the given edition_id
        :param uploaded_to_dataset_api: Whether the metadata has been uploaded to the Dataset API
        :param uploaded_to_upload_service: Whether the data has been uploaded to the Upload Service
        :param error_message: Any error message associated with the event
        :param retry_count: The retry count, if this is a retry event
        :param last_retry_timestamp: The timestamp of the last retry attempt, if this is a retry attempt

        :return: models.DatasetStatus
        """
        return models.DatasetStatus(
            _id=ObjectId(),
            dataset_id=dataset_id,
            created_at=created_at,
            updated_at=updated_at,
            file_name=file_name,
            status=status,
            edition_id=edition_id,
            version_id=version_id,
            uploaded_to_dataset_api=uploaded_to_dataset_api,
            uploaded_to_upload_service=uploaded_to_upload_service,
            error_message=error_message,
            events=events,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )


class DatasetFactory:
    @staticmethod
    def create_dataset(
        dataset_id: str,
        created_at: datetime = datetime.now(),
        updated_at: datetime = datetime.now(),
        latest_edition_id: Optional[str] = None,
        latest_version_id: Optional[int] = None,
        statuses: Dict[str, models.DatasetStatus] = {},
    ) -> models.Dataset:
        """
        Create a new Dataset model.

        :param dataset_id: The dataset_id of this dataset
        :param created_at: The timestamp of the creation of the new dataset
        :param updated_at: The timestamp of the last update to the dataset
        :param latest_edition_id: The edition_id of the given dataset_id
        :param latest_version_id: The version_id of the given edition_id
        :param statuses: Field to store status information for this dataset

        :return: models.Dataset
        """
        return models.Dataset(
            _id=ObjectId(),
            dataset_id=dataset_id,
            created_at=created_at,
            updated_at=updated_at,
            latest_edition_id=latest_edition_id,
            latest_version_id=latest_version_id,
            statuses=statuses,
        )
