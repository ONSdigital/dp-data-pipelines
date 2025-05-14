from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from bson.objectid import ObjectId
from enum import Enum

from pydantic_core import PydanticUndefined


class DatasetStatusType(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    FAILED = "failed"
    COMPLETED = "completed"


class DatasetEventType(Enum):
    RECEIVED = "received"
    PROCESSING = "processing"
    UPLOADED = "uploaded"
    FAILED = "failed"
    COMPLETED = "completed"


class DatasetEventData(BaseModel):
    s3_object_key: str
    upload_location: Optional[str] = None  # upload service/dataset api
    additional_data: Optional[Dict[str, Any]] = Field(default_factory=dict)


class DatasetEvent(BaseModel):
    dataset_id: str
    timestamp: datetime
    event_type: DatasetEventType  # received|processing|uploaded|failed|completed
    event_data: Optional[DatasetEventData] = None
    error_message: Optional[str] = None
    retry_count: int  # Add to state mgmt diagrams
    last_retry_timestamp: Optional[datetime] = None  # Add to state mgmt diagrams


# TODO Assign ObjectID for DatasetEvent without creating separate `dataset_events` collection
# class DatasetEventWithID(DatasetEvent):
#     id: Optional[ObjectId] = Field(default=PydanticUndefined, init=False)
#     model_config = ConfigDict(arbitrary_types_allowed=True)


class DatasetStatus(BaseModel):
    dataset_id: str
    created_at: datetime
    # Do we need updated_at here?
    updated_at: datetime
    file_name: str  # Zip file name
    edition_id: Optional[str] = None
    # version_id is set as `str` in state mgmt diagram but used `int` here as this corresponds to Dataset API datatype
    version_id: Optional[int] = None
    uploaded_to_dataset_api: Optional[bool] = False
    uploaded_to_upload_service: Optional[bool] = False
    error_message: Optional[str] = None
    status: Optional[DatasetStatusType] = None  # pending|processing|failed|completed
    events: Optional[List[DatasetEvent]] = Field(default_factory=list)
    retry_count: Optional[int] = None  # Add to state mgmt diagrams
    last_retry_timestamp: Optional[datetime] = None  # Add to state mgmt diagrams


class DatasetStatusWithID(DatasetStatus):
    id: Optional[ObjectId] = Field(default=PydanticUndefined, init=False)
    model_config = ConfigDict(arbitrary_types_allowed=True)


class Dataset(BaseModel):
    # Rename `id` to `dataset_id` in state mgmt diagrams
    dataset_id: str
    latest_edition_id: str
    # `version_id` is set as `str` in state mgmt diagram but used `int` here as this corresponds to Dataset API datatype
    latest_version_id: int
    created_at: datetime
    updated_at: Optional[datetime] = None
    # status_ids: Optional[List[UUID4]] = Field(default_factory=list)  # DatasetStatus.id
    # statuses: Optional[List["DatasetStatus"]] = Field(default_factory=list)
    # TODO `statuses` is a dict with key `DatasetStatusWithID.id` and value `DatasetStatus`
    statuses: Optional[Dict[ObjectId, DatasetStatus]] = Field(default_factory=dict)
    model_config = ConfigDict(arbitrary_types_allowed=True)
