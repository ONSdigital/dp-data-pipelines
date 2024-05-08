# devnote: not using strtobool from disutils as that
# package is being depreciate from the standard
# library in python >3.12
import os
from dpytools.email.ses.client import SesClient
from email_validator import EmailNotValidError, validate_email


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


class NopEmailClient:
    def send(self, *args, **kwargs):
        print("Email feature is turned off. No email was sent.")


def get_email_client():
    emails_disabled = os.environ.get("DISABLE_EMAILS", "True")
    emails_disabled = str_to_bool(emails_disabled)

    if emails_disabled:
        return NopEmailClient()

    ses_email_identity = os.environ["SES_EMAIL_IDENTITY"]
    email_client = SesClient(ses_email_identity, "eu-west-2")

    return email_client


def get_submitter_email() -> str:
    """
    Placeholder function to be updated once we know where the dataset_id can be extracted from (not necessarily s3_object_name as suggested by argument name)
    """

    # Temporary email address for testing purposes
    # Needs to be updated once we know where the submitter email can be extracted from

    os.environ["TEMPORARY_SUBMITTER_EMAIL"] = "submitter@test.com"

    submitter_email = os.getenv("TEMPORARY_SUBMITTER_EMAIL")

    if submitter_email is None:
        raise NotImplementedError("Submitter email address cannot yet be acquired.")

    try:
        validate_email(submitter_email)
    except EmailNotValidError as e:
        raise ValueError(f"Invalid email address: {submitter_email}. Error: {str(e)}")

    return submitter_email
