import pytest
from _pytest.monkeypatch import monkeypatch
import os

from dpypelines.pipeline.shared.utility import enrich_online
from dpypelines.pipeline.shared.message import unexpected_error


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
