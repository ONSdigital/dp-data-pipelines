# State management

Pipeline successes and failures are recorded in an AWS DocumentDB database comprising two collections - `datasets` and `statuses`. Using Pydantic models, `Dataset` and `Status` documents are validated to ensure the integrity of the data recorded during pipeline operations. These models can be viewed in [db_models.py](../dpypelines/pipeline/db/db_models.py).

## Collections

The `datasets` collection records overarching information about pipeline submissions, such as `dataset_id`, `edition_id` and `version_id` (which correspond to the `dataset_id`, `edition` and `version` values used by the Dataset API). The `datasets` collection also includes a `statuses` field, where each status corresponds to a single pipeline run. `Status` documents are written to the `statuses` collection and then copied into the `statuses` field of the `Dataset` document.

AWS DocumentDB is compatible with MongoDB, which is a NoSQL database. As such, it is schemaless, with data validation performed by Pydantic. A MongoDB [ObjectId](https://pymongo.readthedocs.io/en/stable/api/bson/objectid.html#module-bson.objectid) is assigned as the primary key for `Dataset` and `Status` documents, as well as `Event` objects. The tables below outline the document structure for each collection, and the relationship between `Dataset` and `Status` documents.

### Dataset document structure

| Field name          | Description                                                    | Data type                                                                   |
|:--------------------|:---------------------------------------------------------------|:----------------------------------------------------------------------------|
| `id`                | UUID added automatically during `Dataset` document creation    | ObjectID                                                                    |
| `dataset_id`        | The dataset ID                                                 | str                                                                         |
| `latest_edition_id` | The latest edition ID                                          | int                                                                         |
| `latest_version_id` | The latest version ID                                          | str                                                                         |
| `created_at`        | The timestamp of when the `Dataset` document was first created | datetime                                                                    |
| `updated_at`        | The timestamp of when the `Dataset` document was last updated  | datetime                                                                    |
| `statuses`          | All statuses associated with this dataset                      | Dict[ObjectID, [DatasetStatus](../dpypelines/pipeline/db/db_models.py#L81)] |

### Status document structure

| Field name                   | Description                                                                                                                                                            | Data type                                                        |
|:-----------------------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|:-----------------------------------------------------------------|
| `id`                         | UUID added automatically during `Status` document creation. This is used as the key for the associated status information in the `Dataset` document's `statuses` field | ObjectID                                                         |
| `dataset_id`                 | The dataset ID                                                                                                                                                         | str                                                              |
| `file_name`                  | The name of the `.zip` file uploaded to the S3 ingest bucket                                                                                                           | str                                                              |
| `created_at`                 | The timestamp of when the `Status` document was first created                                                                                                          | datetime                                                         |
| `updated_at`                 | The timestamp of when the `Status` document was last updated                                                                                                           | datetime                                                         |
| `status`                     | The current `Status` type                                                                                                                                              | [DatasetStatusType](../dpypelines/pipeline/db/db_models.py#L10)  |
| `edition_id`                 | The edition ID                                                                                                                                                         | str                                                              |
| `version_id`                 | The version ID                                                                                                                                                         | int                                                              |
| `uploaded_to_dataset_api`    | Whether the metadata has been uploaded to the Dataset API                                                                                                              | bool                                                             |
| `uploaded_to_upload_service` | Whether the data has been uploaded to the Upload Service                                                                                                               | bool                                                             |
| `error_message`              | If the pipeline has failed, the error message associated with the failure                                                                                              | str                                                              |
| `events`                     | The list of pipeline events associated with the current status                                                                                                         | List[[DatasetEvent](../dpypelines/pipeline/db/db_models.py#L59)] |
| `retry_count`                | The number of times pipeline processing has been retried, in the event of pipeline failure                                                                             | int                                                              |
| `last_retry_timestamp`       | The timestamp of the last retry attempt                                                                                                                                | datetime                                                         |

### Event document structure

Events are not stored in their own collection, but an `ObjectID` is assigned to them during database operations for improved observability.

| Field name             | Description                                                                                | Data type                                                      |
|:-----------------------|:-------------------------------------------------------------------------------------------|:---------------------------------------------------------------|
| `id`                   | UUID added automatically during database operations                                        | ObjectID                                                       |
| `dataset_id`           | The dataset ID                                                                             | str                                                            |
| `timestamp`            | The timestamp of the event                                                                 | datetime                                                       |
| `event_type`           | The event type                                                                             | [DatasetEventType](../dpypelines/pipeline/db/db_models.py#L17) |
| `event_data`           | Supplementary event data (`s3_object_name`, `upload_location`, `additional_data`)          | [DatasetEventData](../dpypelines/pipeline/db/db_models.py#L49) |
| `error_message`        | If the pipeline has failed, the error message associated with the failure                  | str                                                            |
| `retry_count`          | The number of times pipeline processing has been retried, in the event of pipeline failure | int                                                            |
| `last_retry_timestamp` | The timestamp of the last retry attempt                                                    | datetime                                                       |

### Document creation

When a new submission arrives in the AWS S3 ingest bucket, the `dataset_id` is extracted from the `.zip` file name. The `datasets` collection is queried to check whether a corresponding `Dataset` document already exists, and if not, a new document is created. A new `Status` document is added to the `statuses` collection, and inserted into the `statuses` field in the `Dataset` document, using the `Status`'s `ObjectID` as a key.

During pipeline processing, the `Status` document is updated with relevant information about processing tasks, such as uploads to the Dataset API and Upload Service. If at any point the pipeline fails, this is recorded in the `Status` document, including any pertinent error information, and pipeline operation stops.

### Database model factory methods

[Pydantic models](../dpypelines/pipeline/db/db_models.py) are available for all database objects. [Factory methods](../dpypelines/pipeline/db/db_model_factories.py) are provided for most common scenarios, such as creating `Dataset`, `DatasetStatus`, `DatasetEvent` and `DatasetEventData` models. These Factory methods include sensible defaults for many fields.

## Database operations

Database operations are managed in the following three classes.

### `DatasetsService`

The [`DatasetsService` class](../dpypelines/pipeline/db/datasets_service.py) handles interactions which update both the `datasets` and the `statuses` collections. You can instantiate a `DatasetsService` object using `DatasetsServiceFactory.create_datasets_service()`. The `datasets` and `statuses` collections can also be accessed directly via the `DatasetsService` object. This function accepts a `DocumentDBClient` object as an argument:

```python
from dpytools.db.documentdb_client import DocumentDBClientOptions, DocumentDBClient
from dpypelines.pipeline.db.db_collection_factories import DatasetsServiceFactory

client_options = DocumentDBClientOptions(host="localhost", port="27017")
client = DocumentDBClient(client_options=client_options, database_name="datasets")
client.connect()

datasets_service = DatasetsServiceFactory.create_db_datasets_service(client=client)
```

The following methods are available:

#### `create_new_dataset()`

This method creates a new `Status` document, and then creates a new `Dataset` document with the newly-created `Status` as the sole entry in the `statuses` field. It returns a `Dataset` model:

```python
new_dataset = datasets_service.create_new_dataset(
    dataset_id="new_dataset_id",
    s3_object_key="bucket/input/new_dataset_id.zip"
)
```

#### `update_dataset_existing_status()`

This method is used to update an existing `Status` document as new events occur during pipeline processing. It returns a `Dataset` model:

```python
from dpypelines.pipeline.db.db_model_factories import (
    DatasetEventFactory,
    DatasetEventDataFactory,
)

updated_dataset = datasets_service.update_dataset_existing_status(
    dataset_id="new_dataset_id",
    status_oid=ObjectId("0123456789ab0123456789ab"),
    event=DatasetEventFactory.create_processing_dataset_event(
        dataset_id="new_dataset_id",
        event_data=DatasetEventDataFactory.create_dataset_event_data(
            s3_object_key="input/new_dataset_id.zip"
        ),
    ),
    new_status=DatasetStatusType.PROCESSING
)
```

#### `update_dataset_new_status()`

This method is used to add a new `Status` to the `statuses` field of an existing `Dataset` document. The new status should already exist in the `statuses` collection. It returns a `Dataset` model.

```python
new_status = datasets_service.statuses_collection.create_new_status(
    dataset_id="dataset_id",
    s3_object_key="bucket/input/dataset_id.zip",
    file_name="dataset_id.zip"
)

updated_dataset = datasets_service.update_dataset_new_status(
    dataset_id="dataset_id",
    new_status=new_status
)
```

#### `create_dataset_if_not_exists()`

This method checks the `datasets` collection to see if a `Dataset` document already exists for the given `s3_object.dataset_id`. If the document exists, a new `Status` document is created in the `statuses` collection, and the new status is appended to the `statuses` field of the `Dataset` document. If the document does not exist, new `Dataset` and `Status` documents are created in the collections. It returns the new `Dataset` model and the `ObjectId` of the new `Status` document:

```python
from dpypelines.pipeline.process_zip_file import S3Object

s3_object = S3Object("bucket/input/new_dataset_id.zip")

new_dataset, status_oid = datasets_service.create_dataset_if_not_exists(s3_object)
```

### `DatasetsCollection`

The `DatasetsCollection` class handles all database operations that relate to updating the `datasets` collection. The following methods are available:

#### `get_dataset()`

This method queries the `datasets` collection for the given `dataset_id`. It returns a `Dataset` model if a match is found.

```python
dataset = datasets_service.datasets_collection.get_dataset(dataset_id="dataset_id")
```

#### `get_all_datasets()`

This method returns a list of `Dataset` models for all documents in the `datasets` collection.

```python
all_datasets = datasets_service.datasets_collection.get_all_datasets()
```

#### `create_dataset()`

This method inserts a new `Dataset` document into the `datasets` collection.

```python
from dpypelines.pipeline.db.db_model_factories import DatasetFactory

dataset = DatasetFactory.create_dataset(
    dataset_id="dataset_id"
)

datasets_service.create_dataset(dataset_model=dataset)
```

#### `dataset_exists()`

This method checks the `datasets` collection for a document matching the given `dataset_id`. It returns `True` if the document exists, and `False` if it does not exist.

```python
dataset_exists = datasets_service.datasets_collection.dataset_exists(dataset_id="dataset_id")
```

#### `update_dataset()`

This method updates a `Dataset` document with the given `update_values` and returns the updated `Dataset` model.

```python
dataset = datasets_service.datasets_collection.get_dataset(dataset_id="dataset_id")

update_values = {
    "key_to_update_1": "new_value_1",
    "key_to_update_2": "new_value_2",
    ...
}

updated_dataset = datasets_service.datasets_collection.update_dataset(
    dataset_model=dataset,
    update_values=update_values
)
```

### `DatasetStatusesCollection`

The `DatasetsStatusesCollection` class handles all database operations that relate to updating the `statuses` collection. The following methods are available:

#### `get_status()`

This method queries the `statuses` collection for the given `status_oid`. It returns a `DatasetStatus` model if a match is found.

```python
status = datasets_service.statuses_collection.get_status(status_oid=ObjectId("0123456789ab0123456789ab"))
```

#### `get_all_statuses_for_dataset_id()`

This method returns a list of `DatasetStatus` models for the given `dataset_id`.

```python
all_statuses = datasets_service.statuses_collection.get_all_statuses_for_dataset_id(dataset_id="dataset_id")
```

#### `create_new_status()`

This method creates a new `Status` document in the `statuses` collection. It returns a `DatasetStatus` model of the newly-created status.

```python
new_status = datasets_service.statuses_collection.create_new_status(
    s3_object_key="input/dataset_id.zip",
    dataset_id="dataset_id",
    file_name="dataset_id.zip
)
```

#### `update_status()`

This method updates an existing `Status` document for the given `status_oid` with the specified `event` and `new_status`. It returns a `DatasetStatus` model of the updated `Status` document. The example below demonstrates how to update the `Status` document for an upload event.

```python
from dpypelines.pipeline.db.db_model_factories import (
    DatasetEventFactory,
    DatasetEventDataFactory,
)

event_data = DatasetEventDataFactory.create_dataset_event_data(
    s3_object_key="input/dataset_id.zip",
    upload_location=models.UploadLocation.UPLOAD_SERVICE
)

event = DatasetEventFactory.create_uploaded_dataset_event(
    dataset_id="dataset_id",
    event_data=event_data
)

updated_status = datasets_service.statuses_collection.update_status(
    status_oid=ObjectId("0123456789ab0123456789ab"),
    event=event,
    new_status=models.DatasetStatusType.PROCESSING
)
```
<!---TODO Add retry lambda information--->
<!---"The retry Lambda (***TODO Link to retry lambda docs***) is configured to periodically check the database for failed submissions, and automatically rerun the pipeline on these submissions if certain conditions are met (***TODO What conditions***)."--->