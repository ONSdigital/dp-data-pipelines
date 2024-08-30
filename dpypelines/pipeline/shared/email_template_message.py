from dataclasses import dataclass


@dataclass
class EmailContent():
    subject: str
    message: str


def file_not_found_email(file_name: str) -> EmailContent:
    """
    Create a subject and message for when a file is not found.
    """
    subject = "Data Submission Error: Missing Required File"
    message = f"The file {file_name} that you submitted could not be found. Please check the file and try again."
    return EmailContent(subject, message)


def supplementary_distribution_not_found_email(distribution_name: str) -> EmailContent:
    """
    Create a subject and message for when a supplementary distribution is not found.
    """
    subject = "Data Submission Error: Missing Supplementary Distribution"
    message = f"The supplementary distribution {distribution_name} that you submitted could not be found. Please check your submission and try again."
    return EmailContent(subject, message)


def submission_processed_email() -> EmailContent:
    """
    Create a subject and message for when a submission has been processed.
    """
    subject = "Data Submission: Processed Successfully"
    message = "Your data submission has been processed. If there were any issues, you will receive a separate email with the details."
    return EmailContent(subject, message)
