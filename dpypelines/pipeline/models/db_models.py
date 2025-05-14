import json
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime, date
from bson.objectid import ObjectId
from enum import Enum
from pydantic import BaseModel, ConfigDict, ValidationError
from pydantic_core import PydanticUndefined
from pyobjectID import PyObjectId, MongoObjectId

class DatasetStatusType(Enum):
    PENDING = 0
    PROCESSING = 1
    FAILED = 2
    COMPLETED = 4

class DatasetEventType(Enum):
    RECEIVED = 0
    PROCESSING = 1
    UPLOADED = 2
    FAILED = 4
    COMPLETED = 8

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

class Dataset(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: PyObjectId = Field(
        alias="_id"
    )
    
    dataset_id: str
    latest_edition_id: str
    # `version_id` is set as `str` in state mgmt diagram but used `int` here as this corresponds to Dataset API datatype
    latest_version_id: int
    created_at: datetime = datetime.now()
    updated_at: Optional[datetime] = None
    # status_ids: Optional[List[UUID4]] = Field(default_factory=list)  # DatasetStatus.id
    # statuses: Optional[List["DatasetStatus"]] = Field(default_factory=list)
    # TODO `statuses` is a dict with key `DatasetStatusWithID.id` and value `DatasetStatus`
    statuses: Optional[Dict[ObjectId, "DatasetStatus"]] = Field(default_factory=dict)
    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.dict(), cls=MongoEncoder))
    
class DatasetStatus(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: PyObjectId = Field(
        alias="_id"
    )

    dataset_id: str
    created_at: datetime = datetime.now()
    # Do we need updated_at here?
    updated_at: datetime = datetime.now()
    file_name: str  # Zip file name
    edition_id: Optional[str] = None
    # version_id is set as `str` in state mgmt diagram but used `int` here as this corresponds to Dataset API datatype
    version_id: Optional[int] = None
    uploaded_to_dataset_api: bool = False
    uploaded_to_upload_service: bool = False
    error_message: Optional[str] = None
    status: DatasetStatusType = DatasetStatusType.PENDING
    events: Optional[List["DatasetEvent"]] = Field(default_factory=list)
    retry_count: int = 0
    last_retry_timestamp: Optional[datetime] = None  # Add to state mgmt diagrams

    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.dict(), cls=MongoEncoder))

class DatasetStatusWithID(DatasetStatus):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    id: PyObjectId = Field(
        alias="_id"
    )
    model_config = ConfigDict(arbitrary_types_allowed=True)
    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.dict(), cls=MongoEncoder))

class DatasetEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: PyObjectId = Field(
        alias="_id"
    )

    dataset_id: str
    timestamp: datetime = datetime.now()
    event_type: DatasetEventType = DatasetEventType.PROCESSING
    event_data: Optional["DatasetEventData"] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    last_retry_timestamp: Optional[datetime] = None  # Add to state mgmt diagrams
    
    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.dict(), cls=MongoEncoder))

# TODO Assign ObjectID for DatasetEvent without creating separate `dataset_events` collection
# class DatasetEventWithID(DatasetEvent):
#     id: Optional[ObjectId] = Field(default=PydanticUndefined, init=False)
#     model_config = ConfigDict(arbitrary_types_allowed=True)


class DatasetEventData(BaseModel):
    s3_object_key: str
    upload_location: Optional[str] = None  #
    additional_data: Optional[Dict[str, Any]] = Field(default_factory=dict)
    def dict_for_mongodb(self):
        # Convert to dict and handle enums
        return json.loads(json.dumps(self.dict(), cls=MongoEncoder))