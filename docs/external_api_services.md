# External API services

The pipeline currently interacts with two external API services - the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api) and the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service). Specific details of these services are not covered in this documentation, which is solely intended to outline how the pipeline interacts with them.

## Dataset API

The metadata associated with a given dataset is published to the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api). This is achieved by sending a `POST` request to the `/datasets/{dataset_id}/editions/{edition_id}/versions` endpoint. Before this request is sent, there are a number of validation steps to ensure that all of the required metadata has been provided, and that the expected endpoint exists.

The examples in this section assume that you are connecting to the Dataset API locally, the `.zip` file downloaded from the AWS S3 bucket has been unzipped to the local directory `path/to/files`, and the local directory contains a valid `manifest.json` file.

### `MetadataLoader`

The validation process starts with the creation of a `MetadataLoader` object. This object is instantiated by passing `DatasetAPIService` and `DpLogger` objects as arguments. A number of environment variables are also required to be set (primarily `DATASET_API_URL` and `SERVICE_TOKEN_FOR_UPLOAD`) - see the [config documentation](config.md) for more details of how pipeline configuration variables are populated.

```python
from dpytools.http.api.dataset_api_service import DatasetAPIService
from dpytools.logging.logger import DpLogger
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.metadata.metadata_loader import MetadataLoader
from dpypelines.pipeline.validate_pipeline import validate_manifest

dataset_api_service = DatasetAPIService("http://localhost:22000/datasets")
logger = DpLogger("dp-ingest-pipeline")

metadata_loader = MetadataLoader(
    dataset_api_service=dataset_api_service,
    logger=logger
)

local_store = LocalDirectoryStore("path/to/files")
manifest = validate_manifest(local_store)

metadata = metadata_loader.load_metadata(
    manifest=manifest,
    local_store=local_store
)
```

The `metadata_loader.load_metadata()` method performs the following steps:
1. Checks the `metadata_file` field in `manifest.json` for the metadata file name.
2. Loads the metadata file into a dictionary.
3. Queries the Dataset API to get the latest published version of the metadata.
4. If the `use_previous_metadata` field in `manifest.json` is set to `true`, combines the metadata loaded in Step 2 with the latest published version from Step 3, overwriting fields where specified.
5. Validates that the distribution files listed in the metadata file exist, are not empty, and are one of the [accepted file types](file_specifications#data-files).
6. Creates a `Metadata` model, validated by Pydantic to ensure all required fields are populated and of the correct data type.

### Uploading metadata

Once the metadata has been loaded into a `Metadata` model, there is some additional validation required to ensure that the Dataset API can accept the new version. This is achieved by sending a `GET` request to the `/datasets/{dataset_id}` endpoint, and deserialising the response JSON into a `GetDatasetResponse` model. The following properties are then verified:
- The response must contain a `current` object.
- The `state` field in the `current` object must be set to `published`.
- The `type` field in the `current` object must be set to `static`.
<!---TODO Is state/type being checked currently? GetDatasetResponse.can_publish_new_version not being called anywhere--->

Once these requirements have been verified, a `POST` request is sent to the `/datasets/{dataset_id}/editions/{edition_id}/versions` endpoint, with the JSON-serialised metadata in the request body.

All of the functionality described above is encompassed in the `validate_and_upload_metadata()` method. It returns `True` if the upload is successful, and `False` if not:

```python
from dpypelines.pipeline.api.dataset_api import validate_and_upload_metadata

metadata_uploaded = validate_and_upload_metadata(metadata, dataset_api_service)
```

## Upload Service

The distribution files associated with a given dataset are published to the [dp-upload-service](https://github.com/ONSdigital/dp-upload-service). This is achieved by sending a `POST` request to the `/upload-new` endpoint. Before this request is sent, there are a number of validation steps to ensure that all of the distribution files are valid.

The examples in this section assume that you are connecting to the Upload Service locally, and that the `.zip` file downloaded from the AWS S3 bucket has been decompressed to the local directory `path/to/files`. The `.zip` file should contain a valid `metadata.json` file listing all of the files to be uploaded in the `distributions` array, and all of these files must be present and in one of the [supported formats](file_specifications.md#data-files).

### Uploading data files

The `upload_files()` method sends a `POST` request to the `/upload-new` endpoint, populating the query parameters as necessary for the request to succeed. One `POST` request is sent for each file to be uploaded.

```python
from dpypelines.pipeline.api.upload import create_upload_service_client, upload_files
from dpypelines.pipeline.config.job_config import get_job_config

config = get_job_config()

upload_service_client = create_upload_service_client(config.upload_service_url)

# NB: `files_to_upload` argument would normally be extracted from `metadata.json`, but is specified directly here for the sake of brevity
files_to_upload = [
    Path("path/to/files/distribution.csv"),
    Path("path/to/files/distribution.xlsx")
]

upload_files(
    files_to_upload=files_to_upload,
    config=config,
    upload_client=upload_service_client
)
```