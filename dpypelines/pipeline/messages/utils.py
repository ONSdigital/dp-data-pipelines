import os
from datetime import datetime
from typing import Optional

import pytz
from dpytools.email.ses.client import SesClient
from dpytools.utilities.utilities import str_to_bool
from email_validator import EmailNotValidError, validate_email

from dpypelines.pipeline.job_configuration import secrets_job_config

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

    ses_email_identity = secrets_job_config.ses_email_identity
    if ses_email_identity:
        email_client = SesClient(ses_email_identity, "eu-west-2")

        return email_client
    else:
        raise ValueError(
            "Failed to create email client, ses_email_identity could not be found."
        )


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
    Gets the current commit ID from the environment variable COMMIT_SHA.
    """
    try:
        commit_sha = os.environ["COMMIT_SHA"]
    except KeyError:
        raise EnvironmentError("COMMIT_SHA environment variable is not set.")

    return commit_sha


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


def get_environment() -> str:
    """
    Gets the current environment.
    """
    return os.environ.get("ENVIRONMENT", "Environment not found")


def get_mimetype(file: str) -> Optional[str]:
    # TODO Set default to "application/octet-stream"?
    return MIMETYPES.get(file, None)
