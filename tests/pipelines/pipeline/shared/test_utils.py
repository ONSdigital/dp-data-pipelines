import os

import pytest

from dpypelines.pipeline.shared.utils import (
    get_florence_access_token,
    get_supplementary_distribution_file,
    str_to_bool,
)


def test_str_to_bool_valid_values():
    """
    Test str_to_bool utility can correctly identify the expected
    boolean values from strings.
    """

    assert str_to_bool("True") is True
    assert str_to_bool("true") is True
    assert str_to_bool("  True ") is True
    assert str_to_bool("False") is False
    assert str_to_bool("false") is False
    assert str_to_bool("false  ") is False


def test_str_to_bool_valid_raises_for_invlaid_str_value():
    """
    Test str_to_bool utility will raise when given a non
    expected string value
    """

    with pytest.raises(ValueError) as err:
        str_to_bool("foo")

    assert (
        "A str value representing a boolean should be one of 'True', 'true', 'False', 'false'"
        in str(err)
    )


def test_str_to_bool_raises_for_not_string_argument():
    """
    Test str_to_bool utility will raise when given a non
    string argument
    """

    for invalid_type in [1, True, 897.23]:

        with pytest.raises(AssertionError) as err:
            str_to_bool(invalid_type)

        assert "Function str_to_bool only accepts strings" in str(err)


def test_florence_access_token_set_as_env_var():
    mp = pytest.MonkeyPatch()
    mp.setenv("FLORENCE_TOKEN", "florence-access-token")
    florence_token = get_florence_access_token()
    assert florence_token == "florence-access-token"


def test_florence_access_token_not_set_as_env_var():
    mp = pytest.MonkeyPatch()
    env_var = os.environ.get("FLORENCE_TOKEN", None)
    if env_var is not None:
        mp.delenv("FLORENCE_TOKEN")
    florence_token = get_florence_access_token()
    assert florence_token == "not-implemented"


def test_get_supplementary_distribution_files():
    files = ["data.xml", "data.csv", "data.xls"]
    matching, extension = get_supplementary_distribution_file(files, "^data.xml$")
    assert matching == "data.xml"
    assert extension == ".xml"


def test_get_supplementary_distribution_files_more_then_one_match():
    files = ["data.xml", "data.csv", "data.xls", "data.xml"]
    with pytest.raises(AssertionError) as err:
        get_supplementary_distribution_file(files, "^data.xml$")
    assert (
        "Error finding file matching pattern ^data.xml$: matching files are ['data.xml', 'data.xml']"
        in str(err.value)
    )


def test_get_supplementary_distribution_files_no_match():
    files = ["data.csv", "data.xls"]
    with pytest.raises(AssertionError) as err:
        get_supplementary_distribution_file(files, "^data.xml$")
    assert (
        "Error finding file matching pattern ^data.xml$: matching files are []"
        in str(err.value)
    )
