# File specifications

This page outlines the requirements for files submitted to the pipeline. The pipeline is triggered when one or more `.zip` files are uploaded to the designated AWS S3 bucket. Each `.zip` file **must** include a `manifest.json` file for configuring the pipeline, and a `metadata.json` file comprising the metadata for the submitted dataset, as well as at least one distribution file in a [supported format](#data-files).

## Manifest and metadata files

We have an [OpenAPI schema definition](/docs/schemas/openapi-specification.yaml) for `metadata.json` and `manifest.json` files. The definition contains schemas for the two JSON files. It also contains two mock routes, for non-existent API endpoints. This allows you to easily view or create example JSON for each file type.

### Viewing the OpenAPI specification file

The best way to view the schema is using Swagger Editor:
1. Copy the URL for the raw schema definition by [opening it in GitHub](/docs/schemas/openapi-specification.yaml), and then clicking the `Raw` button just above the document in the top right.
2. Go to [Swagger Editor](https://editor.swagger.io/).
3. Click `File` -> `Import URL`.
4. Paste in the URL from step 1 and click `OK`.

## Data files

The Pipeline is currently configured to accept data files only in the formats permitted by the [dp-dataset-api](https://github.com/ONSdigital/dp-dataset-api). The formats supported are defined in their own [swagger.yaml](https://github.com/ONSdigital/dp-dataset-api/blob/develop/swagger.yaml), in the `Distribution` definition.

At present, the Dataset API supports, and thus the Pipeline accepts, the following data file formats:
 - `csv`
 - `sdmx`
 - `xls`
 - `xlsx`
 - `csdb`
