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

    all_datasets = [r for r in mock_db_operations.datasets_collection.find({})]
    created_dataset = all_datasets[-1]
    created_status = list(created_dataset["statuses"].values())[0]

    assert result
    assert len(all_datasets) > len(mock_db_operations.datasets)
    assert created_status["status"] == "COMPLETED"
    assert len(created_status["events"]) == 5
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

    all_datasets = [r for r in mock_db_operations.datasets_collection.find({})]
    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore

    assert len(all_datasets) > len(mock_db_operations.datasets)
    assert "Required file not found: data.csv" in str(e)
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == "Required file not found: data.csv"

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
    datasets_before = [r for r in mock_db_operations.datasets_collection.find({})]

    from dpypelines.s3_zip_received import start

    result = start(zip_file_object_key)

    all_datasets = [r for r in mock_db_operations.datasets_collection.find({})]
    updated_dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": "dataset_id_1"}
    )
    created_status = list(updated_dataset["statuses"].values())[-1]  # type:ignore

    assert result
    assert len(all_datasets) == len(datasets_before)
    assert created_status["status"] == "COMPLETED"
    assert len(created_status["events"]) == 5

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

    all_datasets = [r for r in mock_db_operations.datasets_collection.find({})]
    updated_dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": "dataset_id_1"}
    )
    created_status = list(updated_dataset["statuses"].values())[-1]  # type:ignore

    assert "File format validation failed for data" in str(e)
    assert len(all_datasets) == len(mock_db_operations.datasets)
    assert created_status["status"] == "FAILED"
    assert len(created_status["events"]) == 3
    assert (
        created_status["error_message"]
        == "File format validation failed for data.not-a-real-file: Extension not-a-real-file is not supported"
    )

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)
