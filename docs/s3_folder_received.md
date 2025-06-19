# `s3_folder_received` pipeline

The `s3_folder_received` pipeline handles datasets that are received as a `.zip` file in an AWS S3 bucket. The pipeline is triggered upon upload of the `.zip` file, which calls the `s3_folder_received.start()` function, with the S3 object name of the folder as an argument. The onwards pipeline is then configured by a combination of file extensions and a `manifest.json` file.

## `manifest.json` file

The `.zip` file submitted to the pipeline **must** contain a file named `manifest.json`, which contains configuration details required for successful pipeline processing of submissions. Details of required fields for a `manifest.json` file can be found in the [file specifications documentation](file_specifications.md#manifest-and-metadata-files).

## `s3_folder_received.start()` function

In order to run the `s3_folder_received.start()` function, you will need to set an environment variable indicating which AWS environment the S3 bucket is located in. To do this, open your terminal and enter the following command:

```bash
export AWS_PROFILE=<aws_profile_value>
```

In order to communicate with the Dataset API and Upload Service, you will need to port forward to these services.

In the example below, `my-bucket/folder/data.zip` is the S3 object name of the `.zip` file containing one `.csv` file (`data.csv`) and two `.json` files (`manifest.json` and `metadata.json`):

`myscript.py`
```python
from dpypelines import s3_folder_received

s3_folder_received.start('my-bucket/folder/data.zip')
```

The `s3_folder_received.start()` function performs the following steps:

1. Downloads the `data.zip` file to a local directory and decompresses it.
2. Retrieves pipeline configuration details for the given dataset from `manifest.json`.
3. Validates the metadata provided in `metadata.json` and publishes a new version to the Dataset API.
4. Validates the `data.csv` file and uploads it to the Upload Service.

When processing begins, the `.zip` file is moved into the `processing` folder in the S3 bucket. If the pipeline completes with no errors, the file is moved again to the `processed` folder in S3.
