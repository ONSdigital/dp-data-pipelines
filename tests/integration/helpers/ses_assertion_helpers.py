from moto.core.models import DEFAULT_ACCOUNT_ID
from moto.ses.models import ses_backends

from tests.integration.helpers.file_helpers import FILE_AUTHOR_EMAIL


def get_sent_emails():
    ses_backend = ses_backends[DEFAULT_ACCOUNT_ID]["eu-west-2"]
    messages = ses_backend.sent_messages
    return messages


def assert_no_emails_sent():
    sent_emails = get_sent_emails()
    assert len(sent_emails) == 0


def assert_email_sent(expected_content_parts: list[str], expected_subject: str):
    sent_emails = get_sent_emails()
    assert len(sent_emails) == 1

    sent_email = sent_emails[0]
    assert sent_email.subject == expected_subject
    assert sent_email.source == FILE_AUTHOR_EMAIL
    for part in expected_content_parts:
        assert part in sent_email.body


def assert_successful_email():
    assert_email_sent(
        [
            "Your pipeline submission has been processed. If there were any issues, you will receive a separate email with the details"
        ],
        "Pipeline Submission: Processed Successfully",
    )


def assert_exception_email_sent(exception_message: str):
    sent_emails = get_sent_emails()
    assert len(sent_emails) == 1

    sent_email = sent_emails[0]

    assert sent_email.source == FILE_AUTHOR_EMAIL
    assert "ETL Pipeline error has occurred" in sent_email.subject
    assert "An error has occurred:" in sent_email.body
    assert exception_message in sent_email.body
