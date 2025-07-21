# Integration tests

This folder contains the integration tests for the ETL Lambda function (starting from [s3_folder_received.py](../../dpypelines/s3_folder_received.py)) to test the full end-to-end process of the Lambda.

External services, and other dependencies, are mocked to emulate external services as closely as possible, while maintaining test isolation and repeatability.

## Test Categories

All test categories should be covered, which are:

- **Success**: Complete pipelinex execution, with all expected file-types and valid data
- **AWS service errors**: S3, SES, and Secrets Manager errors
- **Dataset API errors**: HTTP errors, 404s, validation failures
- **Upload service errors**: HTTP errors, 404s, validation failures
- **File validation errors**: Missing/invalid manifest, metadata, data files
- **Configuration errors**: Invalid environment variables, missing secrets
- **Feature flags**: Testing disabled notifications, emails, uploads

## Test Configuration

Test configuration is handled in the [conftest.py](conftest.py) file, and uses [pytest fixtures](https://docs.pytest.org/en/6.2.x/fixture.html).

## Mocks

### AWS

AWS services are mocked using [moto](https://docs.getmoto.org/).

We currently mock:
- S3
- Secrets Manager
- SES

### Dataset API

The Dataset API is mocked in the [MockAPIResponses class](mocks/mock_api_responses.py).

This uses the Python [responses package](https://github.com/getsentry/responses) to intercept expected HTTP requests by matching:

- The route
- The method
- The request body

The default expected HTTP requests are configured automatically, but can be overridden if needed, e.g. for error testing. For examples of this see [test_dataset_api_errors.py](test_dataset_api_errors.py).

### Upload Service

The Upload Service is mocked in the [MockAPIResponses class](mocks/mock_api_responses.py).

This uses the Python [responses package](https://github.com/getsentry/responses) to intercept expected HTTP requests by matching:

- The route
- The method
- The request body

The default expected HTTP requests are configured automatically, but can be overridden if needed, e.g. for error testing. For examples of this see [test_upload_service_errors.py](test_upload_service_errors.py).

### Slack Notifications

Slack notifications are also mocked using the same process as Upload Service; we mock the `SlackMessenger` class from the [dpytools](https://github.com/ONSdigital/dp-python-tools) library, and also make it return a success or error result depending on the test.

This should also be changed to use the same process as the Dataset API service mock when possible, but given that it is a simple HTTP request to a webhook this should be acceptable for now.

### DocumentDB/MongoDB

For DocumentDB mocking we use [TestContainers](https://testcontainers.com/), which is configured in [conftest.py](conftest.py). We set up a real MongoDB database in a Docker container, which is automatically cleaned up after the tests.

This is currently setting up _one_ container for the entire test run, but this should possibly be changed to one container per module, or even container per test.

## Test data generation

Test data is generated using a few components:

- **[`FileGenerationConfig`](conftest.py)** is used for configuring the `metadata`, `manifest` and `data_file`. It has various fields which are used to configure the generated file, such as generating an invalid file, completely removing it, removing mandatory values, etc.
- **[`zip_file_object_key_factory`](conftest.py)** is used for actually generating the file above. By default it will generate a valid file for each element, but this can be overridden by customising the `FileGenerationConfig` as described above. For examples see [`test_manifest_file_errors.py`](test_manifest_file_errors.py).
- **[`file_helpers.py`](helpers/file_helpers.py)** contains methods used for generating the required files using `FileGenerationConfig`.

## Test environment configuration

Environment variables, and mock AWS Secrets Manager secret configuration, are managed in [`conftest.py`](conftest.py). This is done by:

### Environment variables

- `DEFAULT_ENV_VARS`: a dictionary of the default (valid) environment variables.
- `configure_env_vars`: a method that actually sets the environment variables for the tests.
- `aws_credentials`: a method to set mock AWS credentials for the mock testing.

### Secrets

- `setup_secrets`: a method that sets default (valid) secrets in the mocked AWS Secrets Manager.

## Assertions

There are reusable assertion helpers in the [helpers](helpers/) folder, for frequently used assertion cases. These include:

- **[notification_assertion_helpers.py](./helpers/notification_assertion_helpers.py)**: common assertions for notification successes/failures.
- **[s3_assertion_helpers.py](./helpers/s3_assertion_helpers.py)**: common assertions for checking whether an S3 file does/doesn't exist in a specific location.
- **[ses_assertion_helpers.py](./helpers/ses_assertion_helpers.py)**: common assertions for checking whether an email was sent using SES or not.
- **[upload_service_assertion_helpers.py](./helpers/upload_service_assertion_helpers.py)**: common assertions for verifying whether the Upload Service was called with the expected parameters.
