import os
from unittest.mock import patch

import pytest

from dpypelines.pipeline.messages.utils import (
    get_commit_id,
    get_mimetype,
    get_submitter_email,
)


def test_get_commit_id():
    """
    Test that get_commit_id correctly retrieves the COMMIT_SHA from the environment.
    """
    with patch.dict(os.environ, {"COMMIT_SHA": "123456789abcdef"}):
        utils_commit_hash = get_commit_id()
        assert utils_commit_hash == "123456789abcdef"


def test_get_commit_id_missing_env_var():
    """
    Test that get_commit_id raises an error if COMMIT_SHA is not set.
    """
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(EnvironmentError) as e:
            get_commit_id()
        assert "COMMIT_SHA environment variable is not set." in str(e.value)


def test_get_submitter_email():
    manifest_dict = {
        "manifestVersion": 1,
        "source_id": "test_move",
        "fileAuthorEmail": "test@email.com",
        "fileAuthorUsername": "Username",
        "isPublishable": False,
        "licence": "My licence",
        "licenceUrl": "http://www.example.org/licence",
        "title": "4g Coverage",
        "aliasName": "4g-coverage",
    }
    email = get_submitter_email(manifest_dict)
    assert email == "test@email.com"


def test_get_submitter_email_invalid():
    manifest_dict = {
        "manifestVersion": 1,
        "source_id": "test_move",
        "fileAuthorEmail": "invalid_email.com",
        "fileAuthorUsername": "Username",
        "isPublishable": False,
        "licence": "My licence",
        "licenceUrl": "http://www.example.org/licence",
        "title": "4g Coverage",
        "aliasName": "4g-coverage",
    }
    with pytest.raises(ValueError) as e:
        get_submitter_email(manifest_dict)

    assert "Invalid email address" in str(e)


def test_get_submitter_email_not_provided():
    manifest_dict = {
        "manifestVersion": 1,
        "source_id": "test_move",
        "fileAuthorEmail": None,
        "fileAuthorUsername": "Username",
        "isPublishable": False,
        "licence": "My licence",
        "licenceUrl": "http://www.example.org/licence",
        "title": "4g Coverage",
        "aliasName": "4g-coverage",
    }
    with pytest.raises(NotImplementedError) as e:
        get_submitter_email(manifest_dict)

    assert "Submitter email address not found." in str(e)


def test_get_mimetype_valid():
    mimetype = get_mimetype(".csv")
    assert mimetype == "text/csv"


def test_get_mimetype_invalid():
    mimetype = get_mimetype(".pdf")
    assert mimetype is None
