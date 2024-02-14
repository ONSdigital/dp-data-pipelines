import pytest
from pipeline.shared.functions.message import (
    unexpected_error, 
    cant_find_scheama,
    invlaid_config,
    unknown_pipeline,
    metadata_validation_error,
    expected_local_file_missing
)


def test_unexpected_error():
    try:
        assert 1 == 2
    except Exception as e:
        human_readable_output = unexpected_error(e.message, e)

    # assert human_readable_output == "An unexpected error occured with error message: {msg}"
    assert type(human_readable_output) == str
