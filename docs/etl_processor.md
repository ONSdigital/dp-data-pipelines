# `ETLProcessor`

The [`ETLProcessor`](../dpypelines/pipeline/etl_processor.py) class encapsulates all of the functionality required for successful pipeline operation. This includes:
- Retrieving job configuration details from AWS Secrets Manager and local environment variables.
- Creating client classes for:
    - Logging;
    - Email and Slack notifications;
    - Dataset API and Upload Service request handling;
    - State management database operations.
- Creating an [`S3Object`](file_processing.md#s3object) from the given `s3_object_name`.

To instantiate an `ETLProcessor` object, pass the name of the S3 object to be processed. You must be signed in to AWS via SSO so that the object can be downloaded.

In the example below, the S3 object `dataset_id.zip` is stored in an AWS S3 bucket called `bucket`, inside a folder called `input`:

```python
from dpypelines.pipeline.etl_processor import ETLProcessor

etl_processor = ETLProcessor(s3_object_name="bucket/input/dataset_id.zip")
```

The following methods are available.

## `process_s3_object_event()`

This method takes a `status_oid` argument, which is the `ObjectId` of the `Status` document to be updated with pipeline events during processing. It calls the [`get_processed_zip_file()`](#get_processed_zip_file) method to get a `ProcessedZipFile` object, and then calls [`process_metadata()`](#process_metadata) on this object.

It returns a value of `True` if the processing is successful (including uploading the metadata to the Dataset API, and uploading the data file(s) to the Upload Service). If `SKIP_DATA_UPLOAD` is set to `True`, but all files are successfully validated, it returns `False`. In the event of a processing error, the [`__handle_exception()`](#__handle_exception) method is called.

```python
from bson.objectid import ObjectId

status_oid = ObjectId("0123456789ab0123456789ab")

s3_object_processed = etl_processor.process_s3_object_event(status_oid=status_oid)
```

## `get_processed_zip_file()`

This method takes a `status_oid` argument, which is the `ObjectId` of the `Status` document to be updated with pipeline events during processing. It processes the `.zip` file by downloading it from S3, decompressing it to a local temporary directory, and validating the `manifest.json` file. It returns a `ProcessedZipFile` object.

```python
processed_zip_file = etl_processor.get_processed_zip_file(status_oid=status_oid)

# Returns a `ProcessedZipFile` object:
# processed_zip_file.s3_object = S3Object("bucket/input/dataset_id.zip")
# processed_zip_file.decompressed_file_dir = Path("tmp/dataset_id")
# processed_zip_file.local_store = LocalDirectoryStore("tmp/dataset_id")
# processed_zip_file.manifest = Manifest(
#    metadata_file="metadata.json",
#    submission_contacts=[SubmissionContact(email="test@example.org")],
#    use_previous_metadata=False
#)
```

## `process_metadata()`

This method takes `processed_zip_file` and `status_oid` arguments.

If `SKIP_DATA_UPLOAD` is set to `False`, the metadata contained in `metadata.json` is loaded and [validated](validation.md#manifest-and-metadata-validation), and and then uploaded to the Dataset API. The [`handle_successful_metadata_upload()`](#handle_successful_metadata_upload) method is then called to validate and upload the data file(s) to the Upload Service. A value of `True` is returned if all uploads are successful.

If `SKIP_DATA_UPLOAD` is set to `True`, the metadata is validated, but no upload requests are made, and the method returns `False`.

In the event of a pipeline error, the [`__handle_exception()`](#__handle_exception) method is called.

```python
metadata_processed = etl_processor.process_metadata(
    processed_zip_file=processed_zip_file,
    status_oid=status_oid
)
```

## `handle_successful_metadata_upload()`

This method takes `processed_zip_file`, `metadata` and `status_oid` arguments.

The names of the files to be uploaded are extracted from `metadata.distributions`. These files are then uploaded to the Upload Service. If the upload is successful, a confirmation email is sent to the data submitter, and a success notification is sent to the Slack channel.

```python
metadata = MetadataLoader(
    etl_processor.dataset_api_service,
    etl_processor.logger).load_metadata(
            processed_zip_file.manifest,
            processed_zip_file.local_store
)

handle_successful_metadata_upload(
    processed_zip_file=processed_zip_file,
    metadata=metadata,
    status_oid=status_oid
)
```

## `__handle_exception()`

In the event of an error during pipeline processing, the `__handle_exception()` method is called. This method utilises the `error_handler()` method described in the [error handling](error_handling.md) documentation.

## Service client methods

There are three service client methods which handle the creation of client classes:
- `get_dataset_api_service()`: Handles [Dataset API](external_api_services.md#dataset-api) requests and responses.
- `get_upload_service_client()`: Handles [Upload Service](external_api_services.md#upload-service) requests and responses.
- `get_db_datasets_service()`: Handles [state management](state_management.md) database operations.

These clients are configured from job configuration variables. See the [config](config.md) documentation for more information.

## State management database operations

Throughout pipeline processing, the state management database is updated with information about pipeline events. See the [state management](state_management.md) documentation for more information.