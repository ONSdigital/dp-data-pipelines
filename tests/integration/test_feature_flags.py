from moto import mock_aws
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.ses_assertion_helpers import (
    assert_no_emails_sent,
    assert_successful_email,
)
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_notifications_disabled(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    monkeypatch,
):
    """
    Test notifications are not sent if disabled
    """
    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "True")
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    result = start(zip_file_object_key)

    # Assertions
    assert result

    mock_api_responses.assert_all_requests_made()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "COMPLETED"
    assert len(status["events"]) == 5

    spy_notifier.assert_not_called()
    spy_notifier_instance = spy_notifier.instance
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_not_called()

    monkeypatch.setenv("DISABLE_NOTIFICATIONS", "False")
    assert_successful_email()


@mock_aws
def test_emails_disabled(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    monkeypatch,
):
    """
    Tests emails are not sent if disabled
    """
    monkeypatch.setenv("DISABLE_EMAILS", "True")

    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    result = start(zip_file_object_key)

    # Assertions
    assert result is True

    mock_api_responses.assert_all_requests_made()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "COMPLETED"
    assert len(status["events"]) == 5

    spy_notifier.assert_called_once()
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_called_once()
    spy_notifier_instance.failure.assert_not_called()

    assert_no_emails_sent()
    monkeypatch.setenv("DISABLE_EMAILS", "False")


# Test upload disabled
@mock_aws
def test_file_upload_disabled(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    monkeypatch,
):
    """
    Tests uploads do not happen if disabled
    """
    monkeypatch.setenv("SKIP_DATA_UPLOAD", "True")
    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    result = start(zip_file_object_key)

    # Assertions
    assert not result

    mock_api_responses.assert_get_versions_called(times=1)
    mock_api_responses.assert_get_dataset_called(times=0)
    mock_api_responses.assert_post_versions_called(times=0)
    mock_api_responses.assert_upload_service_called(times=0)

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "COMPLETED"
    assert len(status["events"]) == 3
    assert status["events"][-1]["event_data"]["additional_data"] == {
        "Info": "Data upload skipped"
    }

    spy_notifier.assert_called_once()
    spy_notifier_instance = spy_notifier.instance
    spy_notifier_instance.success.assert_not_called()
    spy_notifier_instance.failure.assert_not_called()

    assert_no_emails_sent()
    monkeypatch.setenv("SKIP_DATA_UPLOAD", "False")
