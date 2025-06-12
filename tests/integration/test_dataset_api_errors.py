import pytest
from moto import mock_aws
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.conftest import MockAPIResponses
from tests.integration.helpers.notification_assertion_helpers import (
    assert_no_success_and_one_failure,
)
from tests.integration.helpers.ses_assertion_helpers import (
    assert_email_sent,
)
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_get_versions_404_error(
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
    Test 404 error retrieving dataset version from Dataset API
    """
    # Run the pipeline with the S3 object name
    mock_api_responses.mock_get_versions(status_code=404)

    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "GET failed with status code: 404" in str(e)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == "GET failed with status code: 404"

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)

    assert_email_sent(
        [
            "An error has occurred:",
            "GET failed with status code: 404",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        "ETL Pipeline error has occurred",
    )


@mock_aws
def test_post_json_error(
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
    Test when dataset API has a non-success response when uploading the metadata
    """
    # Run the pipeline with the S3 object name
    mock_api_responses.mock_post_versions(status_code=500)

    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert "POST failed with status code: 500" in str(e.value)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "FAILED"
    assert len(status["events"]) == 3
    assert status["error_message"] == "POST failed with status code: 500"

    assert_no_success_and_one_failure(spy_notifier)

    mock_api_responses.assert_all_dataset_api_requests_made()
    mock_api_responses.assert_upload_service_called(times=0)

    assert_email_sent(
        [
            "An error has occurred:",
            "POST failed with status code: 500",
            f"Additional Data: {{'s3_object_name': '{s3_object.name}', 'level': 'INFO'}}",
        ],
        "ETL Pipeline error has occurred",
    )
