# devnote: not using strtobool from disutils as that
# package is being depreciate from the standard
# library in python >3.12
import os
import re

from dpytools.http.upload import UploadClient


def str_to_bool(should_be_bool: str) -> bool:
    """
    Take a string that should represent a boolean
    and convert it to bool.

    Raise if we've an unexpected value.
    """

    assert isinstance(
        should_be_bool, str
    ), f"Function str_to_bool only accepts strings, got {type(should_be_bool)}"

    consistant_should_be_bool = should_be_bool.strip().lower()

    if consistant_should_be_bool == "true":
        return True
    elif consistant_should_be_bool == "false":
        return False
    else:
        raise ValueError(
            f"A str value representing a boolean should be one of 'True', 'true', 'False', 'false'. Got '{should_be_bool}'"
        )


def get_florence_access_token() -> str:
    """
    Get Florence access token from environment variable, if set, otherwise return "not-implemented"
    """
    florence_access_token = os.environ.get("FLORENCE_TOKEN", None)
    if florence_access_token is not None:
        return florence_access_token
    return "not-implemented"


def create_upload_client(upload_url: str) -> UploadClient:
    """
    Create an UploadClient from the specified upload_url
    """
    return UploadClient(upload_url)


def get_supplementary_distribution_file(files: list[str], pattern: str) -> str:
    """
    Get single supplementary distribution filename and file extension from a list of files.

    Raise if more than one file matching pattern found.
    """
    matching_files = [f for f in files if re.search(pattern, f)]
    assert (
        len(matching_files) == 1
    ), f"Error finding file matching pattern {pattern}: matching files are {matching_files}"
    _, extension = os.path.splitext(matching_files[0])
    return matching_files[0], extension
