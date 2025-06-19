# Data pipelines lambdas

This folder contains the Python files for our various Lambda handlers. For a full process overview please see the [dataflow and component architecture](docs/architecture/data_flow_and_component_architecture.md) documentation.

## [lambda_triggers_etl](lambda_triggers_etl/)

This is triggered by an S3 `PutObject` event, and its only purpose is to trigger the `lambda_runs_etl` Lambda below.

## [lambda_runs_etl](lambda_runs_etl/)

This is the main Lambda for the pipeline. Essentially it:

- Downloads the S3 object that it was invoked with.
- Unzips and validates the contents of the `.zip` file.
- Uploads the metadata to the Dataset API.
- Uploads the data file(s) to the Upload Service.

For futher detail please see the rest of the documentation in the [docs](../docs/) folder, particularly [etl_lambda_process.md](../docs/architecture/etl_lambda_process.md).