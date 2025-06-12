from unittest.mock import MagicMock, patch

import mongomock
from pymongo.errors import ServerSelectionTimeoutError
import pytest

from dpypelines.pipeline.db.db_model_factories import DatasetFactory
from dpypelines.s3_zip_received import start


@patch("dpypelines.s3_zip_received.ETLProcessor")
def test_start_succeeds(
    mock_ETLProcessor,
):
    mock_etl_processor = MagicMock(name="ETLProcessor")
    mock_ETLProcessor.return_value = mock_etl_processor
    status_oid = mongomock.ObjectId()
    mock_etl_processor.db_datasets_service = MagicMock()
    mock_etl_processor.db_datasets_service.create_dataset_if_not_exists.return_value = (
        DatasetFactory.create_dataset("dataset_id_1"),
        status_oid,
    )
    mock_etl_processor.process_s3_object_event.return_value = True

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    s3_object_processed = start(s3_object_name)

    assert s3_object_processed
    mock_etl_processor.db_datasets_service.create_dataset_if_not_exists.assert_called_once_with(
        s3_object=mock_etl_processor.s3_object
    )
    mock_etl_processor.process_s3_object_event.assert_called_once_with(status_oid)


@patch("dpypelines.s3_zip_received.ETLProcessor")
def test_start_fails_db_error(
    mock_ETLProcessor,
):
    mock_etl_processor = MagicMock(name="ETLProcessor")
    mock_ETLProcessor.return_value = mock_etl_processor
    mock_etl_processor.db_datasets_service = MagicMock()
    mock_etl_processor.db_datasets_service.create_dataset_if_not_exists.side_effect = (
        ServerSelectionTimeoutError
    )

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    with pytest.raises(ServerSelectionTimeoutError) as e:
        start(s3_object_name)

    mock_etl_processor.db_datasets_service.create_dataset_if_not_exists.assert_called_once_with(
        s3_object=mock_etl_processor.s3_object
    )
    mock_etl_processor.process_s3_object_event.assert_not_called()
    assert "ServerSelectionTimeoutError" in str(e)


@patch("dpypelines.s3_zip_received.ETLProcessor")
def test_start_fails_processing_error(
    mock_ETLProcessor,
):
    mock_etl_processor = MagicMock(name="ETLProcessor")
    mock_ETLProcessor.return_value = mock_etl_processor
    status_oid = mongomock.ObjectId()
    mock_etl_processor.db_datasets_service = MagicMock()
    mock_etl_processor.db_datasets_service.create_dataset_if_not_exists.return_value = (
        DatasetFactory.create_dataset("dataset_id_1"),
        status_oid,
    )
    mock_etl_processor.process_s3_object_event.return_value = False

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    s3_object_processed = start(s3_object_name)

    assert not s3_object_processed
    mock_etl_processor.db_datasets_service.create_dataset_if_not_exists.assert_called_once_with(
        s3_object=mock_etl_processor.s3_object
    )
    mock_etl_processor.process_s3_object_event.assert_called_once_with(status_oid)
