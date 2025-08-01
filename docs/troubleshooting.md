# Troubleshooting

This page deals with all errors that can occur during pipeline operation. Most errors are likely to be caused by incorrectly named or formatted files. Please see the [file specifications](file_specifications.md) and [validation](validation.md) pages for more information.

## User errors

If any of the errors below occur, it is most likely due to an invalid submission being uploaded to the S3 bucket. In the event of any of these errors occurring, an email will be sent to the submission contact listed in the `manifest.json` file with details of the error and advice on how to resolve it. The state management database, Slack notifications and pipeline logs will also contain this information.

### `manifest.json` issues

| Scenario                                          | Exception                  | Error message                                                                                              | Function                               | Module                 |
|:--------------------------------------------------|:---------------------------|:-----------------------------------------------------------------------------------------------------------|:---------------------------------------|:-----------------------|
| `manifest.json` missing                           | `FileNotFoundError`        | Failed to retrieve manifest from the local directory store.                                                | `validate_manifest()`                  | `validate_pipeline.py` |
| `manifest.json` missing field                     | `ValueError`               | Manifest schema validation failed                                                                          | `validate_manifest_schema()`           | `validate_pipeline.py` |
| `manifest.json` invalid JSON                      | `json.JSONDecodeError`     | Expecting value: line 1 column 1 (char 0)                                                                  | `validate_manifest()`                  | `validate_pipeline.py` |
| `manifest.json` empty                             | `json.JSONDecodeError`     | Expecting value: line 1 column 1 (char 0)                                                                  | `validate_manifest()`                  | `validate_pipeline.py` |
| `manifest.json` incorrectly named `metadata_file` | `FileNotFoundError`        | Required file not found: {file_path.name}                                                                  | `validate_file_exists_and_not_empty()` | `validation/utils.py`  |
| `manifest.json` invalid submitter email address   | `pydantic.ValidationError` | 1 validation error for Manifest: value is not a valid email address: An email address must have an @-sign. | `validate_manifest()`                  | `validate_pipeline.py` |


### `metadata.json` issues

| Scenario                                   | Exception                  | Error message                                                                 | Function                                                  | Module                |
|:-------------------------------------------|:---------------------------|:------------------------------------------------------------------------------|:----------------------------------------------------------|:----------------------|
| `metadata.json` missing                    | `FileNotFoundError`        | Required file not found: {file_path.name}                                     | `validate_file_exists_and_not_empty()`                    | `validation/utils.py` |
| `metadata.json` missing required field     | `pydantic.ValidationError` | 1 validation error for MinimalMetadata: Field required                        | `MetadataLoader.load_metadata()`                          | `metadata_loader.py`  |
| `metadata.json` invalid JSON               | `json.JSONDecodeError`     | File {file_name} is not valid JSON: Expecting value: line 1 column 1 (char 0) | `MetadataLoader.load_metadata()`                          | `metadata_loader.py`  |
| `metadata.json` empty                      | `ValueError`               | File is empty: {file_path.name}                                               | `validate_file_exists_and_not_empty()`                    | `validation/utils.py` |
| `dataset_id` does not exist in Dataset API | `Exception`                | GET failed with status code: 404                                              | `MetadataLoader.__get_latest_version_metadata_from_api()` | `metadata_loader.py`  |
| `edition` does not exist in Dataset API    | `Exception`                | GET failed with status code: 404                                              | `MetadataLoader.__get_latest_version_metadata_from_api()` | `metadata_loader.py`  |


### Distribution file issues

| Scenario                                           | Exception           | Error message                                         | Function                                      | Module                |
|:---------------------------------------------------|:--------------------|:------------------------------------------------------|:----------------------------------------------|:----------------------|
| Distribution file missing                          | `FileNotFoundError` | Required file not found: {file_path.name}             | `validate_file_exists_and_not_empty()`        | `validation/utils.py` |
| Distribution file empty                            | `ValueError`        | File is empty: {file_path.name}                       | `validate_file_exists_and_not_empty()`        | `validation/utils.py` |
| Distribution file can't be read as given file type | `ValueError`        | File format validation failed for {distribution.file} | `MetadataLoader.validate_distribution_file()` | `metadata_loader.py`  |
| Distribution file type not accepted                |                     |                                                       |                                               |                       |
| Distribution file too big                          |                     |                                                       |                                               |                       |

### Zip file issues

#### Incorrectly formatted zip file name

Datasets submitted through the Tracker should use a consistent format for the `.zip` file name (`{dataset_id} - {timestamp}.zip`). If files are not named consistently, this may cause issues with processing, as many pipeline components depend on constituent parts of the `s3_object_name`. See the [file processing](file_processing.md#s3object) documentation for more details.

#### Extraneous files added during zipping

e.g. .DS_Store and __MACOSX on Mac

## Technical issues

Missing secrets/env vars

Version state is `associated` but not changed to `approved`/`published` before new version submitted

Service outage:
- S3
- Lambda
- Secrets Manager
- DocumentDB
- SES
- Slack
- Dataset API
- Upload Service