from moto import mock_aws
import pytest
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.ses_assertion_helpers import (
    get_sent_emails,
)
from tests.integration.mocks.mock_api_responses import MockAPIResponses
from tests.integration.mocks.mock_db_operations import MockDBOperations


@mock_aws
def test_slack_notification_error(
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
    Tests when a Slack notification fails
    """

    def throw_error(error_message: str):
        raise Exception(error_message)

    mock_slack.msg_str.side_effect = throw_error

    from dpypelines.s3_zip_received import start

    zip_file_object_key, _ = zip_file_object_key_factory()
    s3_object = S3Object(zip_file_object_key)

    with pytest.raises(Exception) as e:
        start(zip_file_object_key)

    assert ":white_check_mark: , commit ID: some-git-commit" in str(e)

    mock_api_responses.assert_all_requests_made()

    dataset = mock_db_operations.datasets_collection.find_one(
        {"dataset_id": s3_object.dataset_id}
    )
    status = list(dataset["statuses"].values())[0]  # type:ignore
    assert status["status"] == "COMPLETED"
    assert len(status["events"]) == 5

    assert len(spy_notifier.call_args_list) == 1
    spy_notifier_instance = spy_notifier.instances[0]
    spy_notifier_instance.success.assert_called_once()

    sent_emails = get_sent_emails()
    assert len(sent_emails) == 1
