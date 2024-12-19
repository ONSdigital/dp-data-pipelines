import subprocess
import sys

import pytest

from dpypelines.pipeline.shared.utils import (
    get_commit_id,
    get_mimetype,
    get_submitter_email,
)


def test_get_commit_id():
    git_cli_commit_hash = (
        subprocess.check_output(["git", "log", "-1", "--format=%H"])
        .strip()
        .decode("utf-8")
    )
    utils_commit_hash = get_commit_id()
    assert git_cli_commit_hash == utils_commit_hash


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
