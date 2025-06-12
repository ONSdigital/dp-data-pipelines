import os
import sys
from pathlib import Path
from unittest.mock import patch

from _pytest.monkeypatch import MonkeyPatch
import mongomock
import pytest

from dpypelines.pipeline.db.dataset_statuses_collection import DatasetStatusesCollection
from dpypelines.pipeline.db.datasets_collection import DatasetsCollection
from dpypelines.pipeline.db.datasets_service import DatasetsService
from tests.pipelines.pipeline.mocks import (
    MockDatasetDBCollection,
    MockStatusDBCollection,
)

# Add repo root path for imports
repo_root = Path(__file__).parent.parent
sys.path.append(str(repo_root.absolute()))

# Dev note:
# test logic assumes the webhook and Florence token env vars are
# not set. So unset them for the length of
# tests in the event they are currently set.
mp = MonkeyPatch()
mp.setenv("DISABLE_NOTIFICATIONS", "True")
for potential_env_var_name in [
    "DE_SLACK_WEBHOOK",
    "FLORENCE_TOKEN",
]:
    env_var = os.environ.get(potential_env_var_name, None)
    if env_var is not None:
        mp.delenv(potential_env_var_name)


@pytest.fixture
@patch("dpypelines.pipeline.db.dataset_statuses_collection.DocumentDBClient")
def mock_statuses_collection(mock_DocumentDBClient):
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client
    mock_DocumentDBClient.get_collection.return_value = MockStatusDBCollection(
        mock_mongo_client.state.dataset_statuses
    )
    return DatasetStatusesCollection(mock_DocumentDBClient)


@pytest.fixture
def mock_status_db_collection(mock_statuses_collection):
    db_collection = mock_statuses_collection.initialise_collection()
    return MockStatusDBCollection(db_collection)


@pytest.fixture
@patch("dpypelines.pipeline.db.dataset_statuses_collection.DocumentDBClient")
def mock_datasets_collection(mock_DocumentDBClient):
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client
    mock_DocumentDBClient.get_collection.return_value = MockDatasetDBCollection(
        mock_mongo_client.state.datasets
    )
    return DatasetsCollection(mock_DocumentDBClient)


@pytest.fixture
def mock_dataset_db_collection(mock_datasets_collection):
    db_collection = mock_datasets_collection.initialise_collection()
    return MockDatasetDBCollection(db_collection)


@pytest.fixture
def mock_datasets_service_unit(
    mock_datasets_collection,
    mock_dataset_db_collection,
    mock_statuses_collection,
    mock_status_db_collection,
):
    mock_datasets_service = DatasetsService(
        mock_datasets_collection, mock_statuses_collection
    )
    return mock_datasets_service
