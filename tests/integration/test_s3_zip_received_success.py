from moto import mock_aws
import pytest
import responses
from dpypelines.pipeline.metadata.metadata_models import DatasetVersion
from dpypelines.pipeline.process_zip_file import S3Object
from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
    generate_metadata_dict,
)
from tests.integration.helpers.ses_assertion_helpers import (
    assert_successful_email,
)
from tests.integration.mocks.mock_api_responses import (
    MockAPIResponses,
    mock_versions,
)
from tests.integration.mocks.mock_db_operations import MockDBOperations

files_to_generate = ["csvfile.csv", "sqlite.csdb", "excel.xls", "otherexcel.xlsx"]


@responses.activate
@pytest.mark.parametrize("data_file_name", files_to_generate)
@mock_aws
def test_successful_pipeline_execution_using_existing_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    data_file_name,
):
    """
    Test a successful pipeline execution with mocked services.
    """

    keys_to_remove_from_metadata = ["alerts", "quality_designation", "usage_notes"]
    zip_file_object_key, _ = zip_file_object_key_factory(
        data_file_name=data_file_name,
        metadata_config=FileGenerationConfig(
            missing_field_keys=keys_to_remove_from_metadata
        ),
    )
    s3_object = S3Object(zip_file_object_key)

    new_metadata = generate_metadata_dict(data_file_name)
    for key in keys_to_remove_from_metadata:
        del new_metadata[key]
    original_metadata = mock_versions[0]

    combined_metadata = original_metadata.copy()
    combined_metadata.update(new_metadata)
    combined_metadata = DatasetVersion(**combined_metadata).model_dump()

    mock_api_responses.mock_post_versions(request_body=combined_metadata)

    from dpypelines.s3_zip_received import start

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

    assert_successful_email()


@pytest.mark.parametrize("data_file_name", files_to_generate)
@mock_aws
def test_successful_pipeline_execution_with_new_metadata(
    zip_file_object_key_factory,
    setup_secrets,
    ses_mock,
    mock_slack,
    utils_email_validator_mock,
    mock_api_responses: MockAPIResponses,
    mock_db_operations: MockDBOperations,
    spy_notifier,
    data_file_name,
):
    """
    Test a successful pipeline execution with mocked services.
    """
    expected_metadata = DatasetVersion(
        **generate_metadata_dict(data_file_name)
    ).model_dump()

    zip_file_object_key, _ = zip_file_object_key_factory(
        manifest_config=FileGenerationConfig(
            missing_field_keys=["use_previous_metadata"]
        ),
        data_file_name=data_file_name,
    )
    s3_object = S3Object(zip_file_object_key)

    mock_api_responses.mock_post_versions(request_body=expected_metadata)

    from dpypelines.s3_zip_received import start

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

    assert_successful_email()
