# Unit tests

This folder contains unit tests for the functionality in the `dpypelines.pipeline` directory. There are a number of test fixtures in [`conftest.py`](conftest.py) and mock classes in [`mocks.py`](mocks.py), which are used throughout unit tests to simulate external services and database collections and operations.

Additional mocking is also achieved using `unittest.mock` patching and `MagicMock`, as well as the `mongomock` library for MongoDB emulation. `pytest.MonkeyPatch` is used to set environment variables within tests.

All unit tests should verify successful function operation, as well as verifying the correct raising of any errors.

## Mocks

### MockLocalDirectoryStore

Simulates the `dpytools.stores.LocalDirectoryStore`, and its `get_file_names()` method.

### MockStatusesDBCollection

Returns test data that matches the format returned by the `DatasetStatusesCollection` methods.

### MockDatasetDBCollection

Returns test data that matches the format returned by the `DatasetsCollection` methods.
