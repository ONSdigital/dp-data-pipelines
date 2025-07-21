# Validation

All files submitted to the pipleine must be validated to minimise the potential for pipeline failures. There are different validation processes for [JSON](#manifest-and-metadata-validation) and [data files](#data-file-validation), which are described in the sections below.

## Manifest and Metadata validation

Validation of `manifest.json` and `metadata.json` files is handled by [Pydantic](https://docs.pydantic.dev/) when we load the JSON files into the relevant models. These models can be found in [`metadata_models.py`](/dpypelines/pipeline/metadata/metadata_models.py).

Validation and loading of the metadata is handled in [`metadata_loader.py`](../dpypelines/pipeline/metadata/metadata_loader.py). Field validation (e.g. whether a field is mandatory or optional) is designed to comply with the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api). Since we `POST` relevant metadata about the new dataset version to the Dataset API, our validation of `metadata.json` matches their specification; for example, if the `/datasets/{DATASET_ID}/editions/{EDITION_ID}/versions` endpoint has a mandatory field in the request body, then we define this field as mandatory in the model.

Validation and loading of the manifest is handled in [`validate_pipeline.py`](../dpypelines/pipeline/validate_pipeline.py). All `manifest.json` fields are mandatory, as these are all required by the pipeline for operational reasons.

## Data file validation

Validation of data files is handled by the file validator classes in the [`validation/`](../dpypelines/pipeline/validation/) directory. These classes extend the `FileFormatValidator` base class at [`file_format_validator.py`](../dpypelines/pipeline/validation/file_format_validator.py).

File validators are provided for the following file types:
- csv
- excel
- json
- sqlite
- txt (plain text)
- xml

Validation is performed on all files listed in the `distributions` array of `metadata.json`. The following data file properties are validated:
- That the file exists;
- That the file is not empty;
- That the file contents are consistent with the file extension.

Whilst currently we only validate the file format, this could easily be extended to encompass additional validation in future.

The `validate_file_format()` method of each validator class returns a [`ValidationResult`](../dpypelines/pipeline/validation/models.py) object:

```python
from pathlib import Path
from dpypelines.pipeline.validation.csv_validator import CSVValidator

csv_file_path = Path("path/to/data.csv")
csv_validator = CSVValidator()
validation_result = csv_validator.validate_file_format(file_path=csv_file_path)

# If `data.csv` is valid:
# validation_result.valid = True
# validation_result.extension = "csv"
# validation_result.error = "None"

# If `data.csv` is not valid (for example, cannot be read as a CSV file):
# validation_result.valid = False
# validation_result.extension = "csv"
# validation_result.error = "Could not parse path/to/data.csv as a valid CSV"
```
