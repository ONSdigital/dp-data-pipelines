import os
from bson import BSON
import dpypelines.pipeline.models.db_models as models
from pymongo import MongoClient
from datetime import datetime as dt
from insert_test_data import insert_test_data
import crud
from pyobjectID import (generate, PyObjectId, 
                        MongoObjectId, is_valid)

# podman machine start
# docker compose up -d
# Error response from daemon: crun: executable file `./lambda_retry_ingest.py` not found in $PATH

# podman machine set --rootful
# podman machine start
# docker compose up -d
# Error response from daemon: crun: open executable: Permission denied: OCI permission denied

# docker run -d -p 27017:27017 --name mongo-db mongo:latest

mongo_db_connection_string = os.getenv("MONGODB_CONNECTION_STRING", None)

if mongo_db_connection_string is None:
    raise Exception("No connection string")

client = MongoClient(mongo_db_connection_string)
db, datasets, dataset_statuses = insert_test_data(client)

all_datasets = crud.get_all_documents_list(datasets)
all_statuses = crud.get_all_documents_list(dataset_statuses)

# Dataset arrives in S3 bucket
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
object_key = "input/dataset_id_10.zip"
dataset_id = "dataset_id_10"

# TODO Send GET request to Dataset API to get latest_edition_id and latest_version_id?
edition_id = f"edition_id_for_{dataset_id}"
version_id = 1

# Create document in `datasets` collection if not exists
if not datasets.find_one({"dataset_id": dataset_id}):
    dataset = models.Dataset(
        _id=generate(),
        dataset_id=dataset_id,
        latest_edition_id=edition_id,
        latest_version_id=version_id,
        created_at=dt.now(),
    ).dict_for_mongodb()
    
    dataset_object_id = datasets.insert_one(dataset).inserted_id
else:
    dataset_document = datasets.find_one({"dataset_id": dataset_id})
    dataset = models.Dataset.model_validate(dataset_document)

# Create `dataset_event`
dataset_event = models.DatasetEvent(
    _id=generate(),
    dataset_id=dataset_id,
    timestamp=dt.now(),
    event_type=models.DatasetEventType.RECEIVED,
    event_data=models.DatasetEventData(s3_object_key=object_key),
    retry_count=0,
)

# Create `dataset_status` and insert into `dataset_statuses` collection
# TODO Check for existing statuses and append `event` to `events`
dataset_status = models.DatasetStatus(
    _id=generate(),
    dataset_id=dataset_id,
    created_at=dt.now(),
    updated_at=dt.now(),
    file_name=object_key.split("/", 1)[-1],
    edition_id=edition_id,
    version_id=version_id,
    status=models.DatasetStatusType.PENDING,
    events=[dataset_event],
    retry_count=dataset_event.retry_count,
).dict_for_mongodb()

# https://www.mongodb.com/docs/languages/python/pymongo-driver/current/data-formats/custom-types/type-codecs/
db.get_collection("dataset_statuses")

res = dataset_statuses.insert_one(dataset_status)

print(res)
"""
TypeError: document must be an instance of dict, bson.son.SON, bson.raw_bson.RawBSONDocument, or a type that inherits from collections.MutableMapping
"""
