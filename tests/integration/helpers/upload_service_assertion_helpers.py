from typing import Optional
from unittest.mock import MagicMock

from tests.helpers.generators.data_file_generators import get_media_type_for_file_name

DEFAULT_SUCCESSFUL_EXPECTED_CALLS = [
    {"mimetype": "text/csv", "filename": "data.csv"},
]


def generate_expected_call_for_data_file_name(data_file_name: str):
    return {
        "mimetype": get_media_type_for_file_name(data_file_name),
        "filename": data_file_name,
    }


def validate_successful_upload_service_calls(
    mock_upload_service: MagicMock,
    data_file_name: Optional[str] = None,
    expected_calls: Optional[list[dict]] = None,
):
    if expected_calls is None and data_file_name is None:
        expected_calls = DEFAULT_SUCCESSFUL_EXPECTED_CALLS
    elif data_file_name and not expected_calls:
        expected_calls = [generate_expected_call_for_data_file_name(data_file_name)]
    elif data_file_name and expected_calls:
        expected_calls.append(generate_expected_call_for_data_file_name(data_file_name))

    upload_service_calls = mock_upload_service.upload_new.call_args_list

    assert len(upload_service_calls) == 1

    for expected_call in expected_calls:
        found = False
        for actual_call in upload_service_calls:
            found = call_matches(expected_call, actual_call)
            if found:
                break

        assert found is True, f"Failed to find {expected_call} in upload service calls"


def call_matches(expected_call: dict, actual_call):
    args = actual_call.args
    file_matches = args[0].name == expected_call["filename"]
    mimetype_matches = args[1] == expected_call["mimetype"]
    found = file_matches and mimetype_matches
    return found
