import pytest
from _pytest.monkeypatch import monkeypatch

from dpypelines.pipeline.shared.utility import enrich_online, str_to_bool

def test_enrich_decorator(monkeypatch):
    """
    Test we can control the behaviour of the enrich online decorator via an env var.
    """

    @enrich_online
    def _foo_func() -> str:
        return "foo"

    # no env var set
    assert _foo_func() == "foo"

    # creating an enviormental variable for the test
    monkeypatch.setenv("ENRICH_OUTGOING_MESSAGES", "boo")

    # env var is set
    assert _foo_func() == "foo" + " boo"


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

    assert "A str value representing a boolean should be one of 'True', 'true', 'False', 'false'" in str(err)


def test_str_to_bool_raises_for_not_string_argument():
    """
    Test str_to_bool utility will raise when given a non
    string argument
    """

    for invalid_type in [1, True, 897.23]:

        with pytest.raises(AssertionError) as err:
            str_to_bool(invalid_type)

        assert "Function str_to_bool only accepts strings" in str(err)