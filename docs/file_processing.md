# File processing

The pipeline is triggered when one or more `.zip` files are uploaded to the designated AWS S3 bucket. The first step in the pipeline is to download and decompress this `.zip` file to a local directory.

## `S3Object`

When the pipeline is triggered, an `s3_object_name` is passed as an argument to the `s3_zip_received.start()` function. The `s3_object_name` consists of the name of the S3 bucket where the file was uploaded, and an object key consisting of the folder and file name. For example, if the bucket name was `bucket` and the object key was `input/dataset_id.zip`, the `s3_object_name` would be `bucket/input/dataset_id.zip`.

Different components of the pipeline require specific information contained in the `s3_object_name` value to perform correctly, such as the file name, file extension and dataset ID. The `S3Object` class provides a convenient way of extracting these details.

```python
from dpypelines.pipeline.process_zip_file import S3Object

s3_object_name = "bucket/input/dataset_id.zip"

s3_object = S3Object(s3_object_name)

# Returns an S3Object:
# s3_object.name = "bucket/input/dataset_id.zip"
# s3_object.bucket = "bucket"
# s3_object.key = "input/dataset_id.zip"
# s3_object.filename = "dataset_id.zip"
# s3_object.extension = "zip"
# s3_object.folder = "input"
# s3_object.dataset_id = "dataset_id"
```

## `process_zip_file(s3_object)`

The `process_zip_file()` method downloads the `.zip` file from S3 to a local directory and decompresses it. It then returns a `LocalDirectoryStore` object and the path to the decompressed files.

```python
from dpypelines.pipeline.process_zip_file import process_zip_file

local_store, decompressed_files_dir = process_zip_file(
    s3_object=s3_object
)
```

Once the decompressed files are available locally, validation begins with checking that a valid `manifest.json` file has been provided, as this is used to configure onward processing. See the [validation documentation](validation.md#manifest-and-metadata-validation) for more information.
