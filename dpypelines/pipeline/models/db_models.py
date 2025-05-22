import json
from datetime import datetime, date
from enum import Enum
from typing import Any, Dict, List, Optional

from bson.objectid import ObjectId
from pydantic import BaseModel, ConfigDict, Field


class DatasetStatusType(Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class DatasetEventType(Enum):
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    UPLOADED = "UPLOADED"
    FAILED = "FAILED"
    COMPLETED = "COMPLETED"


class UploadLocation(Enum):
    DATASET_API = "DATASET_API"
    UPLOAD_SERVICE = "UPLOAD_SERVICE"


class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Enum):
            return obj.name
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, "DatasetEvent"):
            return obj.dict_for_mongodb()
        if isinstance(obj, "DatasetEventData"):
            return obj.dict_for_mongodb()
        if isinstance(obj, List["DatasetEvent"]):
            return [event.dict_for_mongodb() for event in obj]
        return super().default(obj)


class DatasetEventData(BaseModel):
    s3_object_key: str
    upload_location: Optional[UploadLocation] = None
    additional_data: Optional[Dict[str, Any]] = Field(default_factory=dict)

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))


class DatasetEventDataFactory:
    @staticmethod
    def create_dataset_event_data(
        s3_object_key: str,
        upload_location: Optional[UploadLocation] = None,
        additional_data: Optional[Dict[str, Any]] = None,
    ) -> DatasetEventData:
        return DatasetEventData(
            s3_object_key=s3_object_key,
            upload_location=upload_location,
            additional_data=additional_data,
        )


class DatasetEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: ObjectId = Field(alias="_id")
    dataset_id: str
    timestamp: datetime = datetime.now()
    event_type: DatasetEventType = DatasetEventType.RECEIVED
    event_data: Optional[DatasetEventData] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    last_retry_timestamp: Optional[datetime] = None  # Add to state mgmt diagrams

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))


class DatasetEventFactory:
    @staticmethod
    def create_dataset_event(
        _id: ObjectId,
        dataset_id: str,
        timestamp: datetime,
        event_type: DatasetEventType,
        event_data: Optional[DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetEvent:
        return DatasetEvent(
            _id=_id,
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
        _id: ObjectId,
        dataset_id: str,
        timestamp: datetime,
        event_data: Optional[DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetEvent:
        return DatasetEventFactory.create_dataset_event(
            _id=_id,
            dataset_id=dataset_id,
            timestamp=timestamp,
            event_type=DatasetEventType.RECEIVED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_processing_dataset_event(
        _id: ObjectId,
        dataset_id: str,
        timestamp: datetime,
        event_data: Optional[DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetEvent:
        return DatasetEventFactory.create_dataset_event(
            _id=_id,
            dataset_id=dataset_id,
            timestamp=timestamp,
            event_type=DatasetEventType.PROCESSING,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_uploaded_dataset_event(
        _id: ObjectId,
        dataset_id: str,
        timestamp: datetime,
        event_data: Optional[DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetEvent:
        return DatasetEventFactory.create_dataset_event(
            _id=_id,
            dataset_id=dataset_id,
            timestamp=timestamp,
            event_type=DatasetEventType.UPLOADED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_failed_dataset_event(
        _id: ObjectId,
        dataset_id: str,
        timestamp: datetime,
        event_data: Optional[DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetEvent:
        return DatasetEventFactory.create_dataset_event(
            _id=_id,
            dataset_id=dataset_id,
            timestamp=timestamp,
            event_type=DatasetEventType.FAILED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )

    @staticmethod
    def create_completed_dataset_event(
        _id: ObjectId,
        dataset_id: str,
        timestamp: datetime,
        event_data: Optional[DatasetEventData] = None,
        error_message: Optional[str] = None,
        retry_count: int = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetEvent:
        return DatasetEventFactory.create_dataset_event(
            _id=_id,
            dataset_id=dataset_id,
            timestamp=timestamp,
            event_type=DatasetEventType.COMPLETED,
            event_data=event_data,
            error_message=error_message,
            retry_count=retry_count,
            last_retry_timestamp=last_retry_timestamp,
        )


class DatasetStatus(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: ObjectId = Field(alias="_id")
    dataset_id: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    file_name: str  # Zip file name
    status: DatasetStatusType = DatasetStatusType.PENDING
    edition_id: Optional[str] = None
    # version_id is set as `str` in state mgmt diagram but used `int` here as this corresponds to Dataset API datatype
    version_id: Optional[int] = None
    uploaded_to_dataset_api: bool = False
    uploaded_to_upload_service: bool = False
    error_message: Optional[str] = None
    events: Optional[List[DatasetEvent]] = Field(default_factory=list)
    retry_count: Optional[int] = 0
    last_retry_timestamp: Optional[datetime] = None  # Add to state mgmt diagrams

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))


class DatasetStatusFactory:
    @staticmethod
    def create_dataset_status(
        _id: ObjectId,
        dataset_id: str,
        created_at: datetime,
        updated_at: datetime,
        file_name: str,
        status: DatasetStatusType = DatasetStatusType.PENDING,
        edition_id: Optional[str] = None,
        version_id: Optional[int] = None,
        uploaded_to_dataset_api: bool = False,
        uploaded_to_upload_service: bool = False,
        error_message: Optional[str] = None,
        events: Optional[List[DatasetEvent]] = [],
        retry_count: Optional[int] = 0,
        last_retry_timestamp: Optional[datetime] = None,
    ) -> DatasetStatus:
        return DatasetStatus(
            _id=_id,
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


class Dataset(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: ObjectId = Field(alias="_id")
    dataset_id: str
    latest_edition_id: str
    # `version_id` is set as `str` in state mgmt diagram but used `int` here as this corresponds to Dataset API datatype
    latest_version_id: int
    created_at: datetime = datetime.now()
    updated_at: Optional[datetime] = None
    statuses: Optional[Dict[ObjectId, DatasetStatus]] = Field(default_factory=dict)

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))
