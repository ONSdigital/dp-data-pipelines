# `s3_zip_received` pipeline

The `s3_zip_received` pipeline is triggered upon receipt of a `.zip` file into an AWS S3 bucket, which calls the `s3_zip_received.start()` function, with the S3 object name of the `.zip` file as an argument.

Pipeline operation is configured by a `manifest.json` file which **must** be included in the submitted `.zip` file. See the [file specifications](file_specifications.md#manifest-and-metadata-files) documentation for more information about manifest requirements.

## `s3_zip_received.start()` function

In order to run the `s3_zip_received.start()` function, there are a number of job configuration variables that need to be set. See [the config documentation](config.md) for more information.

In the example below, `bucket/input/dataset_id.zip` is the S3 object name of the `.zip` file containing one `.csv` file (`dataset_id.csv`) and two `.json` files (`manifest.json` and `metadata.json`):

```python
from dpypelines.s3_zip_received import start

s3_object_processed = start('bucket/input/dataset_id.zip')
```

The `s3_zip_received.start()` function performs the following steps:

1. Creates an [`ETLProcessor`](etl_processor.md) object, which retrieves the job configuration details and creates clients for managing notifications and interactions with APIs (e.g. Dataset API and Upload Service).
2. Queries the state management database and creates the necessary documents for tracking pipeline events.
3. Processes the submitted `.zip` file by:
    - Decompressing the file and validating the contents.
    - Submitting the metadata to the Dataset API.
    - Uploading the data files to the Upload Service.
    - Sending Slack and email notifications reporting pipeline success or failure.
    - Updating the state management database as pipeline events occur.

It returns a boolean value of `True` if the submission has been successfully processed.
