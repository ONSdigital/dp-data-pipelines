import json
import pytest
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.file_helpers import FileGenerationConfig
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


def test_new_dataset_start_succeeds(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
):
    zip_file_object_key, _ = zip_file_object_key_factory()

    from dpypelines.s3_zip_received import start

    result = start(zip_file_object_key)

    assert result

    all_datasets = mock_db_operations.get_all_datasets()
    created_dataset = all_datasets[-1]
    assert len(all_datasets) > len(mock_db_operations.datasets)
    mock_db_operations.assert_completed_status_new_dataset(
        dataset_id=created_dataset["dataset_id"], event_count=5
    )

    mock_api_responses.assert_all_requests_made()


def test_new_dataset_start_fails(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
):
    zip_file_object_key, _ = zip_file_object_key_factory(
        data_config=FileGenerationConfig(include=False)
    )
    s3_object = S3Object(zip_file_object_key)

    from dpypelines.s3_zip_received import start

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "Required file not found: data.csv" in str(e)

    all_datasets = mock_db_operations.get_all_datasets()
    assert len(all_datasets) > len(mock_db_operations.datasets)
    mock_db_operations.assert_failed_status_new_dataset(
        dataset_id=s3_object.dataset_id,
        event_count=3,
        err_msg="Required file not found: data.csv",
    )

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)


def test_existing_dataset_start_succeeds(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
):
    zip_file_object_key, _ = zip_file_object_key_factory(dataset_id="dataset_id_1")
    datasets_before = mock_db_operations.get_all_datasets()

    from dpypelines.s3_zip_received import start

    result = start(zip_file_object_key)

    assert result

    all_datasets = mock_db_operations.get_all_datasets()
    assert len(all_datasets) == len(datasets_before)
    mock_db_operations.assert_completed_status_existing_dataset(
        dataset_id="dataset_id_1", event_count=5
    )

    mock_api_responses.assert_all_requests_made()


def test_existing_dataset_start_fails(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
):
    data_contents = {"somekey": "somevalue"}
    extension = "not-a-real-file"
    data_file_name = f"data.{extension}"
    zip_file_object_key, _ = zip_file_object_key_factory(
        data_config=FileGenerationConfig(
            include=True, content=json.dumps(data_contents)
        ),
        data_file_name=data_file_name,
        dataset_id="dataset_id_1",
    )

    from dpypelines.s3_zip_received import start

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "File format validation failed for data" in str(e)

    all_datasets = mock_db_operations.get_all_datasets()
    assert len(all_datasets) == len(mock_db_operations.datasets)

    mock_db_operations.assert_failed_status_existing_dataset(
        dataset_id="dataset_id_1",
        event_count=3,
        err_msg="File format validation failed for data.not-a-real-file: Extension not-a-real-file is not supported",
    )

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)
