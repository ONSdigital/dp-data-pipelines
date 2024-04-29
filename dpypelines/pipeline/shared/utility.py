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

    if consistant_should_be_bool in ["true", "false"]:
        return consistant_should_be_bool == "true"

    raise ValueError(
        f"A str value representing a boolean should be one of 'True', 'true', 'False', 'false'. Got '{should_be_bool}'"
    )


class NopEmailClient:
    def send(self, *args, **kwargs):
        print("Email feature is turned off. No email was sent.")


def get_email_client():
    emails_disabled = os.environ.get("DISABLE_EMAILS", "false")
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

    # What you can use WHILE DEVELOPING only.
    submitter_email = os.getenv("TEMPORARY_SUBMITTER_EMAIL")

    if (
        submitter_email is None
        or "@" not in submitter_email
        or submitter_email.count("@") > 1
    ):
        raise NotImplementedError("Submitter email address cannot yet be acquired.")

    return submitter_email
