# Configuration

Configuration is managed in the [job_config.py](../dpypelines/pipeline/config/job_config.py) file. It uses [pydantic_settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/) to:

1. Read secret values from AWS Secrets Manager.
2. Read values from a local `.env` file.
3. Read values from the OS environment.

The settings are loaded in that order, and are overwritten if a matching key is supplied in a subsequent load. For example, if `EXAMPLE_VARIABLE` is set from AWS Secrets Manager, but `EXAMPLE_VARIABLE` is also set in the `.env` file, then the `.env` variable will take priority.

## Secrets

Pipeline operation in AWS depends on a number of sensitive variables, known in AWS as Secrets. These are managed using AWS Secrets Manager. The current list of Secrets is as follows:

| Secret name                    | Description                                                                                |
|:-------------------------------|:-------------------------------------------------------------------------------------------|
| `DATABASE_CONNECTION_STRING`   | The connection string used to connect to the state management database.                    |
| `DATABASE_NAME`                | The name of the state management database.                                                 |
| `DATASET_API_URL`              | The URL to connect to the Dataset API.                                                     |
| `DE_SLACK_WEBHOOK`             | The webhook to use to send messages to the Slack support channel.                          |
| `LAMBDA_FAILURE_SLACK_WEBHOOK` | The webhook to use to send messages to the Slack channel in the event of a Lambda failure. |
| `SERVICE_TOKEN_FOR_UPLOAD`     | The service token for authenticating requests to the Dataset API and Upload Service.       |
| `SES_EMAIL_IDENTITY`           | The email address from which pipeline notifications should be sent.                        |
| `UPLOAD_SERVICE_URL`           | The URL to connect to the Upload Service.                                                  |

## Environment variables

In addition to the Secrets listed above, there are a number of non-sensitive variables that are required for successful pipeline operation, as follows:

| Environment variable                    | Description                                                                                                            | Example                                           |
|:----------------------------------------|:-----------------------------------------------------------------------------------------------------------------------|:--------------------------------------------------|
| `AWS_PROFILE`                           | The AWS profile name associated with the current environment.                                                          | `dp-sandbox`                                      |
| `ENVIRONMENT`                           | The current environment.                                                                                               | `sandbox`                                         |
| `COMMIT_SHA`                            | The GitHub commit hash denoting which branch is being used for the current pipeline run (used in Slack notifications). | `d9b190e12f5e2c0ff64be94a5bb0c072014573c6`        |
| `DOCKER_HOST`                           | The Docker host for running the MongoDB Testcontainer (for integration testing).                                       | `unix:///path/to/podman-machine-default-api.sock` |
| `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` | Docker socket override for running the MongoDB Testcontainer (for integration testing).                                | `/var/run/docker.sock`                            |
| `TESTCONTAINERS_RYUK_DISABLED`          | Whether to disable Ryuk for Testcontainers (must be set to true if running Docker/Podman in rootless mode).            | `true`                                            |

## Feature flags

The following feature flags can be set to enable/disable specific pipeline functionality:

| Feature flag            | Description                                                                                                                                                                                  | Default |
|:------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|:--------|
| `DISABLE_EMAILS`        | Disable email notifications - if `False`, requires `SES_EMAIL_IDENTITY` to be set, and a valid recipient email address to be provided in `manifest.json`.                                    | `False` |
| `DISABLE_NOTIFICATIONS` | Disable Slack notifications - if `False`, requires `DE_SLACK_WEBHOOK` to be set.                                                                                                             | `False` |
| `SKIP_DATA_UPLOAD`      | Skip upload of data and metadata to the Upload Service and Dataset API respectively - if `False`, requires `DATASET_API_URL`, `UPLOAD_SERVICE_URL` and `SERVICE_TOKEN_FOR_UPLOAD` to be set. | `False` |
