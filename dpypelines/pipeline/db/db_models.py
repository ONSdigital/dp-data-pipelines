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
    def default(self, o):
        if isinstance(o, Enum):
            return o.name
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, date):
            return o.isoformat()
        if isinstance(o, "DatasetEvent"):
            return o.dict_for_mongodb()
        if isinstance(o, "DatasetEventData"):
            return o.dict_for_mongodb()
        if isinstance(o, List["DatasetEvent"]):  # type:ignore
            return [event.dict_for_mongodb() for event in o]
        return super().default(o)


class DatasetEventData(BaseModel):
    s3_object_key: str
    upload_location: Optional[UploadLocation] = None
    additional_data: Optional[Dict[str, Any]] = Field(default_factory=dict)

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))


class DatasetEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: ObjectId = Field(alias="_id")
    dataset_id: str
    timestamp: datetime = datetime.now()
    event_type: DatasetEventType = DatasetEventType.RECEIVED
    event_data: Optional[DatasetEventData] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    last_retry_timestamp: Optional[datetime] = None

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))

    def is_upload_event(self) -> bool:
        if self.event_data is not None and self.event_data.upload_location is not None:
            return True
        return False


class DatasetStatus(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: ObjectId = Field(alias="_id")
    dataset_id: str
    file_name: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    status: DatasetStatusType = DatasetStatusType.PENDING
    edition_id: Optional[str] = None
    version_id: Optional[int] = None
    uploaded_to_dataset_api: bool = False
    uploaded_to_upload_service: bool = False
    error_message: Optional[str] = None
    events: List[DatasetEvent] = Field(default_factory=list)
    retry_count: int = 0
    last_retry_timestamp: Optional[datetime] = None

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))


class Dataset(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: ObjectId = Field(alias="_id")
    dataset_id: str
    created_at: datetime = datetime.now()
    updated_at: datetime = datetime.now()
    latest_edition_id: Optional[str] = None
    latest_version_id: Optional[int] = None
    statuses: Dict[str, DatasetStatus] = Field(default_factory=dict)

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.model_dump(), cls=MongoEncoder))
