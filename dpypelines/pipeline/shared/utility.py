import os

from dpytools.email.ses.client import SesClient

# devnote: not using strtobool from disutils as that
# package is being depreciate from the standard
# library in python >3.12
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


def get_email_client():
    ses_email_identity = os.environ["SES_EMAIL_IDENTITY"]

    # Add a feature flag here, something that pulls from an env var

    # if this feature is flagged on
    email_client = SesClient(ses_email_identity, "eu-west-2")

    # if its not .... make email_client some sort of shallow class
    # that just logs a "I didnt send this email cos flagged off" or similar.

    return email_client


def get_submitter_email() -> str:
    """
    Placeholder function to be updated once we know where the dataset_id can be extracted from (not necessarily s3_object_name as suggested by argument name)
    """

    # What you can use WHILE DEVELOPING only.
    return os.environ["TEMPORARY_SUBMITTER_EMAIL"]

    # What you should put in pr
    raise NotImplementedError("Submitter email address cannot yet be acquired.")
