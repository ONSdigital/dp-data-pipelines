# devnote: not using strtobool from distutils as that
# package is being depreciate from the standard
# library in python >3.12
import os
from datetime import datetime
from typing import Optional

import pytz
from dpytools.email.ses.client import SesClient
from dpytools.utilities.utilities import str_to_bool
from email_validator import EmailNotValidError, validate_email
from git import Repo

MIMETYPES = {
    ".csv": "text/csv",
    ".xml": "application/xml",
    ".json": "application/json",
    ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".csdb": "application/octet-stream",
}


class NopEmailClient:
    def send(self, *args, **kwargs):
        print("Email feature is turned off. No email was sent.")


def get_email_client():
    """
    Creates an email client object to be used for sending notification/error report emails.
    """
    emails_disabled = os.environ.get("DISABLE_EMAILS", "True")
    emails_disabled = str_to_bool(emails_disabled)

    if emails_disabled:
        return NopEmailClient()

    ses_email_identity = os.environ["SES_EMAIL_IDENTITY"]
    email_client = SesClient(ses_email_identity, "eu-west-2")

    return email_client


def get_submitter_email(manifest_dict: dict) -> str:
    """
    This function returns the submitter email from the provided manifest_dict (which is generated from the manifest.json file)
    """

    # Temporary email address for testing purposes
    # Needs to be updated once we know where the submitter email can be extracted from
    submitter_email = manifest_dict["fileAuthorEmail"]

    if manifest_dict["manifestVersion"] != 1:
        raise ValueError(
            f'The manifest version does not match required version(which should be 1) suppllied version: {manifest_dict["manifestVersion"]}.'
        )

    if submitter_email is None:
        raise NotImplementedError("Submitter email address not found.")

    try:
        validate_email(submitter_email)
    except EmailNotValidError as e:
        raise ValueError(f"Invalid email address: {submitter_email}. Error: {str(e)}")

    return submitter_email


def get_commit_id() -> str:
    """
    Gets the current commit ID from the repository.
    """
    try:
        repo = Repo()
    except Exception:
        environment = os.environ["ENVIRONMENT"]
        if not os.path.exists("/tmp/dp-data-pipelines"):
            repo = Repo.clone_from(
                "https://github.com/ONSdigital/dp-data-pipelines.git",
                "/tmp/dp-data-pipelines",
                branch=environment,
            )
        else:
            repo = Repo("/tmp/dp-data-pipelines")

    return str(repo.head.commit)


def get_environment() -> str:
    """
    Gets the current environment.
    """
    try:
        repo = Repo()
        heads = repo.heads
        if "sandbox" in str(heads):
            return "sandbox"
        elif "staging" in str(heads):
            return "staging"
        elif "production" in str(heads):
            return "production"
        else:
            return "Environment is unknown"

    except Exception:
        # found in env variables
        environment = os.environ["ENVIRONMENT"]
        return environment


def get_local_time():
    """
    Utility function for retrieving a string of the date/time.
    """
    # Get the timezone object for London
    tz_London = pytz.timezone("Europe/London")

    # Get the current time in London
    datetime_London = datetime.now(tz_London)

    # Format London date time into hours, minutes, and seconds
    formatted_datetime_London = datetime_London.strftime("%H:%M:%S")

    # Format the time as a string and print it
    return formatted_datetime_London


def get_mimetype(file: str) -> Optional[str]:
    # TODO Set default to "application/octet-stream"?
    return MIMETYPES.get(file, None)
