# devnote: not using strtobool from disutils as that
# package is being depreciate from the standard
# library in python >3.12
import os


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
    Get Florence access token from environment variable, if set, otherwise raise NotImplementedError
    """
    florence_access_token = os.environ.get("FLORENCE_TOKEN", None)
    if florence_access_token is not None:
        return florence_access_token
    raise NotImplementedError("No Florence token set")
