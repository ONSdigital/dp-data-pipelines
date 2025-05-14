from random import randint
from bson import BSON
from bson.raw_bson import RawBSONDocument
from bson.json_util import loads, dumps
import dpypelines.pipeline.models.db_models as models
from pymongo import MongoClient
from datetime import datetime as dt
from dpypelines.pipeline.models.enum_codec import EnumCodec
from bson.codec_options import CodecOptions, TypeRegistry
from dpytools.db.documentdb_client import DocumentDBClient
from bson.objectid import ObjectId

# docker run -d -p 27017:27017 --name mongo-db mongo:latest
client = DocumentDBClient("localhost", 27017)
client.connect(uuidRepresentation="standard")
state_db = client.get_database("state")

# test_datasets = [
#     models.Dataset(
#         dataset_id=f"dataset_id_{i}",
#         latest_edition_id=f"edition_id_for_dataset_id_{i}",
#         latest_version_id=randint(0, 9),
#         created_at=dt.now().isoformat(),
#         statuses={},
#     ).model_dump()
#     for i in range(10)
# ]
# statuses = {
#     "status_object_id_1": {
#         "status": models.DatasetStatusType,
#         "updated_at": dt.now().isoformat(),
#     }
# }
# datasets_collection.create_many_documents(test_datasets)

# test_dataset_statuses = [
#     models.DatasetStatus(
#         dataset_id=f"dataset_id_{i}",
#         created_at=dt.now().isoformat(),
#         updated_at=dt.now().isoformat(),
#         file_name=f"dataset_id_{i}.zip",
#     ).model_dump()
#     for i in range(10)
# ]

"""
event = {
    "Records": [
        {
            "s3": {
                "bucket": {
                    "name": "bucket-name",
                },
                "object": {
                    "key": "input/dataset_id_10.zip",
                },
            },
        }
    ]
}
object_key = event["Records"][0]["s3"]["object"]["key"]
dataset_id = object_key.split("/", 1)[-1].split(".")[0]
"""
s3_object_name = "bucket/input/dataset_id_1.zip"
object_key = "input/dataset_id_1.zip"
dataset_id = "dataset_id_1"

# TODO Send GET request to Dataset API to get latest_edition_id and latest_version_id?
edition_id = f"edition_id_for_{dataset_id}"
version_id = 1

"""
Create document in `datasets` collection if not exists
if not datasets_collection.read_one_document({"dataset_id": dataset_id}):
    dataset = models.Dataset(
        dataset_id=dataset_id,
        latest_edition_id=edition_id,
        latest_version_id=version_id,
        created_at=dt.now().isoformat(),
    )
    dataset_object_id = datasets_collection.create_one_document(
        dataset.model_dump()
    ).inserted_id
else:
    dataset_document = datasets_collection.read_one_document({"dataset_id": dataset_id})
    dataset = models.Dataset.model_validate(dataset_document)"""

status_codec = EnumCodec(enum_class=models.DatasetStatusType, value_class=str)
event_codec = EnumCodec(enum_class=models.DatasetEventType, value_class=str)
type_registry = TypeRegistry([status_codec, event_codec])
codec_options = CodecOptions(type_registry=type_registry)

statuses_collection = client.get_collection(
    db=state_db, collection_name="dataset_statuses", codec_options=codec_options
)
# statuses_collection = client.get_collection(
#     db=state_db, collection_name="dataset_statuses"
# )

# dataset_statuses_collection = client.get_collection(
#     db=state_db, collection_name="dataset_statuses"
# )
# dataset_event_dict = {
#     "dataset_id": dataset_id,
#     "timestamp": dt.now().isoformat(),
#     "event_type": models.DatasetEventType.RECEIVED,
#     "event_data": models.DatasetEventData(s3_object_key=object_key),
#     "retry_count": 0,
# }
# dataset_event_dict_to_model = models.DatasetEvent.model_validate(dataset_event_dict)

# dataset_status_dict = {
#     "dataset_id": dataset_id,
#     "created_at": dt.now().isoformat(),
#     "updated_at": dt.now().isoformat(),
#     "file_name": f"{dataset_id}.zip",
#     "edition_id": edition_id,
#     "version_id": version_id,
#     "status": models.DatasetStatusType.PENDING,
#     "events": [dataset_event_dict],
#     "retry_count": dataset_event_dict["retry_count"],
# }
# dataset_status_dict_to_model = models.DatasetStatus.model_validate(dataset_status_dict)

# Create `dataset_event`
dataset_event = models.DatasetEvent(
    dataset_id=dataset_id,
    timestamp=dt.now().isoformat(),
    # event_type=models.DatasetEventType.RECEIVED,
    event_type=models.DatasetEventType.RECEIVED.value,
    # event_type="received",
    event_data=models.DatasetEventData(s3_object_key=object_key),
    retry_count=0,
)
# dataset_event_model_to_dict = dataset_event_model.model_dump()

# Create `dataset_status` and insert into `dataset_statuses` collection
dataset_status = models.DatasetStatus(
    dataset_id=dataset_id,
    created_at=dt.now().isoformat(),
    updated_at=dt.now().isoformat(),
    file_name=f"{dataset_id}.zip",
    edition_id=edition_id,
    version_id=version_id,
    # status=models.DatasetStatusType.PENDING,
    status=models.DatasetStatusType.PENDING.value,
    # status="pending",
    events=[dataset_event],
    retry_count=dataset_event.retry_count,
)
dataset_status_dict = dataset_status.model_dump()
dataset_status_json = dataset_status.model_dump_json()
# dict_dataset_status = dict(dataset_status)

create_status_result = statuses_collection.create_one_document(dataset_status_dict)
status_document = statuses_collection.read_one_document(
    {"_id": create_status_result.inserted_id}
)
# bson.errors.InvalidBSON: 'dataset_id_1' is not a valid DatasetEventType

res_dict = {
    "_id": ObjectId("68235138033a4346a607911a"),
    "dataset_id": "dataset_id_1",
    "created_at": dt(2025, 5, 13, 15, 3, 35, 219000),
    "updated_at": dt(2025, 5, 13, 15, 3, 35, 219000),
    "file_name": "dataset_id_1.zip",
    "edition_id": "edition_id_for_dataset_id_1",
    "version_id": 1,
    "uploaded_to_dataset_api": False,
    "uploaded_to_upload_service": False,
    "error_message": None,
    "status": "pending",
    "events": [
        {
            "dataset_id": "dataset_id_1",
            "timestamp": dt(2025, 5, 13, 15, 3, 35, 219000),
            "event_type": "received",
            "event_data": {
                "s3_object_key": "input/dataset_id_1.zip",
                "upload_location": None,
                "additional_data": {},
            },
            "error_message": None,
            "retry_count": 0,
            "last_retry_timestamp": None,
        }
    ],
    "retry_count": 0,
    "last_retry_timestamp": None,
}


# TODO Check for existing statuses and append `event` to `events`

# https://www.mongodb.com/docs/languages/python/pymongo-driver/current/data-formats/custom-types/type-codecs/
# db.get_collection("dataset_statuses", codec_options=codec_options)

# res = dataset_statuses.insert_one(
#     BSON.encode(dataset_status.model_dump(), codec_options=codec_options)
# )
# print(res)
"""
TypeError: document must be an instance of dict, bson.son.SON, bson.raw_bson.RawBSONDocument, or a type that inherits from collections.MutableMapping
"""
