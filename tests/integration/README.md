# Integration tests

This folder contains integration tests for the ETL Lambda Function (starting from [dpypelines/s3_folder_received.py](/tests/dpypelines/s3_folder_received.py))

External services are mocked using either Pytest, unit-test, or moto. Some reusable mocks are available in the [mocks][./mocks] folder.

There are various reusable helpers in the [helpers](./helpers) folder, ranging from file creation helpers, to frequently used assertion cases.