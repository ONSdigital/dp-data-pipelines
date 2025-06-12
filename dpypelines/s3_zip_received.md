# `s3_zip_received` pipeline

The `s3_zip_received` pipeline handles datasets that are received in a directory. The pipeline is triggered upon receipt of the directory containing the files into an AWS S3 bucket, which calls the `s3_zip_received.start()` function, with the S3 object name of the folder as an argument. The onwards pipeline is then configured by a combination of file extensions and a `manifest.json` file which **must** be included in `folder-containing-data`.


## `s3_zip_received.start()` function

In order to run the `s3_zip_received.start()` function, there are a number of job configuration variables that need to be set. See [the config documentation](../docs/config.md) for more information.

In the example below, `bucket/input/dataset_id.zip` is the S3 object name of the zip file containing the `.csv` and two `.json` files:

`myscript.py`
```python
from dpypelines import s3_zip_received

s3_zip_received.start('bucket/input/dataset_id.zip')
```

To run this script, open your terminal and enter the following command:

```bash
poetry run python3 ./myscript.py
```

The `s3_zip_received.start()` function performs the following steps:

1. Creates an `ETLProcessor` object, which retrieves the job configuration details and creates clients for managing notifications and interactions with APIs (e.g. Dataset API and Upload Service).
2. Queries the state management database and creates the necessary documents for tracking pipeline events.
3. Processes the submitted zip file by:
    - Unzipping the file and validating the contents.
    - Submitting the metadata to the Dataset API.
    - Uploading the data files to the Upload Service.
    - Updating the state management database with pipeline events.
    - Sending Slack and email notifications.

## `manifest.json` file

The `dataset_id.zip` file submitted to the pipeline **must** contain a file named `manifest.json`, which contains configuration details required for successful pipeline processing of submissions. See the [Pipeline input requirements documentation](../docs/pipeline-process-requirements.md) for more information about manifest requirements.
