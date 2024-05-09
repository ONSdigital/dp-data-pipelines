import pytest

from dpypelines.pipeline.shared.utils import get_florence_access_token, str_to_bool


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


def test_str_to_bool_valid_raises_for_invalid_str_value():
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
    mp.delenv("FLORENCE_TOKEN")
    with pytest.raises(ValueError) as err:
        get_florence_access_token()
    assert "No Florence token set" in str(err.value)
