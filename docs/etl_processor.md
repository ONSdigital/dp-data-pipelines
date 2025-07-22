# `ETLProcessor`

The [`ETLProcessor`](../dpypelines/pipeline/etl_processor.py) class encapsulates all of the functionality required for successful pipeline operation. This includes:
- Retrieving job configuration details from AWS Secrets Manager and local environment variables.
- Creating client classes for:
    - Logging;
    - Email and Slack notifications;
    - Dataset API and Upload Service request handling;
    - State management database operations.
- Creating an `S3Object` from the given `s3_object_name`.
<!---TODO link to file_processing.md#S3Object when merged--->

To instantiate an `ETLProcessor` object, pass the name of the S3 object to be processed. You must be signed in to AWS via SSO so that the object can be downloaded.

In the example below, the S3 object `dataset_id.zip` is stored in an AWS S3 bucket called `bucket`, inside a folder called `input`:

```python
from dpypelines.pipeline.etl_processor import ETLProcessor

etl_processor = ETLProcessor(s3_object_name="bucket/input/dataset_id.zip")
```

The following methods are available.

## `process_s3_object_event()`

This method takes a `status_oid` argument, which is the `ObjectId` of the `Status` document to be updated with pipeline events during processing. It calls the [`get_processed_zip_file()`](#get_processed_zip_file) method to get a `ProcessedZipFile` object, and then calls [`process_metadata_and_distributions()`](#process_metadata_and_distributions) on this object.

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

## `process_metadata_and_distributions()`

This method takes `processed_zip_file` and `status_oid` arguments.

<!---TODO Add links to validation.md when merged--->
If `SKIP_DATA_UPLOAD` is set to `False`, the metadata contained in `metadata.json` is loaded and validated, and  the distributions listed in the metadata are uploaded to the Upload Service. The [`handle_successful_data_upload()`](#handle_successful_data_upload) method is then called to validate and upload the metadata to the Dataset API. A value of `True` is returned if all uploads are successful.

If `SKIP_DATA_UPLOAD` is set to `True`, the metadata is validated, but no upload requests are made, and the method returns `False`.

In the event of a pipeline error, the [`__handle_exception()`](#__handle_exception) method is called.

```python
metadata_processed = etl_processor.process_metadata_and_distributions(
    processed_zip_file=processed_zip_file,
    status_oid=status_oid
)
```

## `handle_successful_data_upload()`

This method takes `metadata` and `status_oid` arguments.

The metadata is submitted to the Dataset API as a `POST` request. If the request is successful, a confirmation email is sent to the data submitter, and a success notification is sent to the Slack channel.

```python
metadata = MetadataLoader(
    etl_processor.dataset_api_service,
    etl_processor.logger).load_metadata(
            processed_zip_file.manifest,
            processed_zip_file.local_store
)

handle_successful_metadata_upload(
    metadata=metadata,
    status_oid=status_oid
)
```

## `__handle_exception()`

<!---TODO Link to error_handling.md when merged--->
In the event of an error during pipeline processing, the `__handle_exception()` method is called. This method utilises the `error_handler()` method described in the [error handling] documentation.

## Service client methods

<!---TODO Add links to API and state mgmt docs when merged--->
There are three service client methods which handle the creation of client classes:
- `get_dataset_api_service()`: Handles Dataset API requests and responses.
- `get_upload_service_client()`: Handles Upload Service requests and responses.
- `get_db_datasets_service()`: Handles state management database operations.

These clients are configured from job configuration variables. See the [config](config.md) documentation for more information.

## State management database operations

<!---TODO Add link to state_management.md when merged--->
Throughout pipeline processing, the state management database is updated with information about pipeline events. See the [state management] documentation for more information.