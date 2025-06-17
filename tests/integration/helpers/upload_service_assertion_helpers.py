from datetime import datetime
from typing import Optional
from unittest.mock import MagicMock

from tests.helpers.generators.data_file_generators import get_media_type_for_file_name


def generate_expected_call_for_data_file_name(data_file_name: str):
    identifier = f"{datetime.now().strftime(format='%d%m%y%H%M%S')}-{data_file_name.replace('.', '-').replace(' ', '_')}"
    return {
        "mimetype": get_media_type_for_file_name(data_file_name),
        "filename": data_file_name,
        "upload_path": f"datasets/{identifier}",
        "identifier": identifier,
    }


DEFAULT_SUCCESSFUL_EXPECTED_CALLS = [
    generate_expected_call_for_data_file_name("data.csv")
]


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

    if expected_calls is None:
        expected_calls = []

    upload_service_calls = mock_upload_service.upload_new.call_args_list

    assert len(upload_service_calls) == len(expected_calls)

    for expected_call in expected_calls:
        found = False
        for actual_call in upload_service_calls:
            found = call_matches(expected_call, actual_call)
            if found:
                break

        assert found is True, f"Failed to find {expected_call} in upload service calls"


def remove_date_time_from_identifier(text: str, replacement="") -> str:
    datetime = text.replace("datasets/", "").split("-")[0]
    return text.replace(datetime, replacement)


def remove_date_time_from_strings_and_compare(
    expected: dict, actual: dict, key: str
) -> bool:
    def get_value_without_datetime(dict: dict):
        return remove_date_time_from_identifier(dict[key])

    return get_value_without_datetime(expected) == get_value_without_datetime(actual)


def call_matches(expected_call: dict, actual_call):
    args = actual_call.kwargs
    file_matches = args["file_path"].name == expected_call["filename"]
    mimetype_matches = args["mimetype"] == expected_call["mimetype"]

    identifier_matches = remove_date_time_from_strings_and_compare(
        args, expected_call, "identifier"
    )
    upload_path_matches = remove_date_time_from_strings_and_compare(
        args, expected_call, "upload_path"
    )

    found = (
        file_matches and mimetype_matches and identifier_matches and upload_path_matches
    )
    return found
