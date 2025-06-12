# Data Transformation and Validation Pipeline input requirements

This document outlines the [input requirements](#inputs) for files to be submitted to the Data Transformation and Validation (DTV) pipeline. All pipeline submissions **must** include a `manifest.json` file for configuring the pipeline, and a `metadata.json` for metadata about the included dataset.

## Inputs

The DTV pipeline is triggered when one or more .zip files are uploaded to the designated AWS S3 bucket. Each .zip file should contain the data file(s) to be published, a `metadata.json` with necessary relevant information about the dataset, and a `manifest.json` file for configuring the pipeline.

### Manifest + Metadata file specifications

We have an OpenAPI schema definition for the `metadata.json` and `manifest.json` files at [openapi-specification.yaml](/docs/schemas/openapi-specification.yaml)

The definition contains schemas for the 2 files. It also contains 2 mock routes, for non-existent API end-points. This allows you to easier view/create an example JSON for each file type.

#### Viewing the OpenAPI specification file

The best way to view the schema is using [Swagger Editor](https://editor.swagger.io/):
1. Go to [Swagger Editor](https://editor.swagger.io/)
2. Copy the URL for the raw schema definition by [opening it in GitHub](./openapi-specification.yaml), and then clicking the `Raw` button just above the document in the top right
3. Click `File` -> `Import URL`
4. Paste in the URL from step 2 and click `OK`

#### Validation

Validation against these files is handled by [pydantic](https://docs.pydantic.dev/) when we load the files from a dictionary into the relevant classes. The data classes for the `metadata.json`, `manifest.json`, and related classes are found in [dpypelines/pipeline/models.py](/dpypelines/pipeline/models.py)

For the `metadata.json`, the field validation, e.g. whether it's required or optional, is designed to [match the dp-dataset-api.](#dp-dataset-api).

The `manifest.json` has all fields mandatory, as they are all required by the Pipeline for various reasons.

Validation is handled in:
- Validation + loading of the metadata, manifest, and that the relevant data file(s) exist is handled in `dpypelines/pipeline/validate_pipeline.py`.

### Data files

The Pipeline is currently setup to accept data files in the formats allowed by the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api). The formats supported are defined in their own [swagger.yaml](https://github.com/ONSdigital/dp-dataset-api/blob/develop/swagger.yaml), in the `Distribution` definition.

Currently it supports, and thus we accept, the following formats:
 - `csv`
 - `sdmx`
 - `xls`
 - `xlsx`
 - `csdb`

#### Validation

Currently the only validation done on each data file received is:
1. Ensuring that the data file(s) mentioned in the `metadata.json`'s `distributions` array exist
2. That the file(s) are not empty
3. That the file(s) match the file format they are defined as. E.g. if they're specified as a `.csv` then we validate that they _are_ a CSV
   
We do not check that data exists in the files, or that the data is "right", for the most part. The only way we might do that is when we validate that the file matches the extension; e.g. we cannot verify a file is a `CSV`, not a `TXT` file if it has no data in it.

Validation on the data file formats use various classes and methods in the `dpypelines/pipeline/validation` folder, which are ran from `dpypelines/pipeline/validation/utils.py`. Each file format has its own class which inherit from a base [FileFormatValidator](dpypelines/pipeline/validation/file_format_validator.py) class. Whilst currently they only validate the file format, this could easily be extended to encompass other validation in future.

## dp-dataset-api

The Pipeline is currently setup to accept data files in the formats allowed by the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api). The formats supported are defined in their own [swagger.yaml](https://github.com/ONSdigital/dp-dataset-api/blob/develop/swagger.yaml), in the `Distribution` definition.

In addition, since we POST relevant metadata to the Dataset API about the new dataset version, our validation on the `metadata.json` matches their specification as well; e.g. if the `/datasets/{DATASET_ID}/editions/{EDITION_ID}/versions` route has a mandatory field in the request body, then we define it as mandatory as well.