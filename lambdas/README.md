# Data pipelines Lambdas

This folder contains the files for the pipeline Lambda handlers. For a full process overview please see the [dataflow and component architecture](../docs/architecture/data_flow_and_component_architecture.md) documentation.

## [lambda_triggers_etl](lambda_triggers_etl/)

This Lambda is triggered by an S3 `PutObject` event, and its only purpose is to call the `lambda_runs_etl` function described below.

## [lambda_runs_etl](lambda_runs_etl/)

This is the main Lambda function for the pipeline. Essentially it calls the `dpypelines.s3_zip_received.start()` function which:

- Downloads the S3 object that it was invoked with.
- Unzips and validates the contents of the `.zip` file.
- Uploads the data file(s) to the Upload Service.
- Uploads the metadata to the Dataset API.

For further details please see the rest of the documentation in the [docs](../docs/) folder, particularly [ETL Lambda Process](../docs/architecture/etl_lambda_process.md).