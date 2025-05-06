from dpytools.db.documentdb_client import DocumentDBClient

import dpypelines.pipeline.models.db_models as models
from dpypelines.pipeline.db.db_collections import DatasetStatusesCollection


client = DocumentDBClient("localhost", 27017)
client.connect(username="root", password="example")
db = client.get_database("db")
collection = client.get_collection(db, "collection")
status_collection = DatasetStatusesCollection(collection, "dataset_id")

object_key = "input/dataset_id_1.zip"
status = status_collection.create_new_status(object_key)

update_status_for_processing_event = status_collection.update_status(
    status.id,
    models.DatasetEventType.PROCESSING,
    models.DatasetStatusType.PROCESSING,
    object_key,
)

all_statuses = status_collection.get_all_statuses_for_dataset_id()

update_status_for_upload_event = status_collection.update_status(
    status.id,
    models.DatasetEventType.UPLOADED,
    models.DatasetStatusType.PROCESSING,
    object_key,
    upload_event=True,
    upload_location="dataset_api",
)

all_statuses = status_collection.get_all_statuses_for_dataset_id()

update_status_for_failure_event = status_collection.update_status(
    status.id,
    models.DatasetEventType.FAILED,
    models.DatasetStatusType.FAILED,
    object_key,
    error_msg="Pipeline failure",
)

all_statuses = status_collection.get_all_statuses_for_dataset_id()
