# devnote: not using strtobool from distutils as that
# package is being depreciate from the standard
# library in python >3.12
import os
from datetime import datetime

import pytz
from dpytools.email.ses.client import SesClient
from dpytools.utilities.utilities import str_to_bool
from email_validator import EmailNotValidError, validate_email
from git import Repo


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


def get_submitter_email(manifest_dict: dict) -> str:
    """
    This function returns the subbmiter email form the provided manifest_dict (which is the data in the manifest.json file)
    """

    # Temporary email address for testing purposes
    # Needs to be updated once we know where the submitter email can be extracted from
    submitter_email = manifest_dict["fileAuthorEmail"]

    if manifest_dict["manifestVersion"] != 1:
        raise ValueError(
            f'The manifest version does not match required version(which should be 1) suppllied version: {manifest_dict["manifestVersion"]}.'
        )

    if submitter_email is None:
        raise NotImplementedError("Submitter email address cannot yet be acquired.")

    try:
        validate_email(submitter_email)
    except EmailNotValidError as e:
        raise ValueError(f"Invalid email address: {submitter_email}. Error: {str(e)}")

    return submitter_email


def get_commit_id() -> str:
    if not os.path.exists("/tmp/dp-data-pipelines"):
        repo = Repo.clone_from(
            "https://github.com/ONSdigital/dp-data-pipelines.git",
            "/tmp/dp-data-pipelines",
        )
    else:
        repo = Repo("/tmp/dp-data-pipelines")
    return str(repo.head.commit)


def get_environment() -> str:
    if not os.path.exists("/tmp/dp-data-pipelines"):
        repo = Repo.clone_from(
            "https://github.com/ONSdigital/dp-data-pipelines.git",
            "/tmp/dp-data-pipelines",
        )
    else:
        repo = Repo("/tmp/dp-data-pipelines")

    heads = repo.heads
    if "sandbox" in str(heads):
        return "sandbox"
    elif "staging" in str(heads):
        return "staging"
    elif "production" in str(heads):
        return "production"
    else:
        return "Environment is unknown"


def get_local_time():
    # Get the timezone object for London
    tz_London = pytz.timezone("Europe/London")

    # Get the current time in London
    datetime_London = datetime.now(tz_London)

    # Format London date time into hours, minutes, and seconds
    formatted_datetime_London = datetime_London.strftime("%H:%M:%S")

    # Format the time as a string and print it
    return formatted_datetime_London
