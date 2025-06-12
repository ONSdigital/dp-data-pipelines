import pytest
from moto import mock_aws
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.file_helpers import FileGenerationConfig
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.ses_assertion_helpers import (
    assert_exception_email_sent,
)
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_upload_service_request_unsupported_filetype(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """
    Test upload error due to unsupported mimetype.
    """
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory(
        data_config=FileGenerationConfig(content="Mimetype unsupported"),
        data_file_name="data.txt",
    )
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(NotImplementedError) as e:
        start(zip_file_object_key)

    assert "Uploading file type .txt not supported for file" in str(e)

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_all_dataset_api_requests_made()
    mock_api_responses.assert_upload_service_called(times=0)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 4
    assert "Uploading file type .txt not supported for file" in status["error_message"]

    # Not desired behaviour
    assert_exception_email_sent(str(e.value))


@mock_aws
def test_upload_service_returns_404_error(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
):
    """
    Test error raise when endpoint not found
    """
    zip_file_object_key, data_file_size = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    mock_api_responses.mock_upload_service(
        file_size=data_file_size,
        status_code=404,
    )

    from dpypelines.s3_zip_received import start

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "404 Client Error: Not Found for url" in str(e)

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_all_requests_made()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 4
    assert (
        "404 Client Error: Not Found for url: http://test-upload-service.url/upload-new"
        in status["error_message"]
    )

    assert_exception_email_sent(str(e.value))
