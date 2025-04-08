from dataclasses import dataclass


@dataclass
class EmailContent:
    subject: str
    message: str


def required_file_not_found_email(required_file_name: str) -> EmailContent:
    """
    Create a subject and message for when a required file is not found.
    """
    subject = "Pipeline Submission Error: File Missing"
    message = f"The file {required_file_name} could not be found. Please check your submission and try again."
    return EmailContent(subject, message)


def supplementary_distribution_not_found_email(distribution_name: str) -> EmailContent:
    """
    Create a subject and message for when a supplementary distribution is not found.
    """
    subject = "Pipeline Submission Error: Supplementary Distribution Missing"
    message = f"The supplementary distribution {distribution_name} could not be found. Please check your submission and try again."
    return EmailContent(subject, message)


def submission_processed_email() -> EmailContent:
    """
    Create a subject and message for when a submission has been processed.
    """
    subject = "Pipeline Submission: Processed Successfully"
    message = "Your pipeline submission has been processed. If there were any issues, you will receive a separate email with the details."
    return EmailContent(subject, message)


def successful_file_upload_email(file: str) -> EmailContent:
    """
    Create a subject and message to confirm a successful file upload.
    """
    subject = "File Upload: Completed Successfully"
    message = f"The file '{file}' has been uploaded successfully."
    return EmailContent(subject, message)


def failed_file_upload_email(file: str, error_info: str) -> EmailContent:
    """
    Create a subject and message to report a file upload failure.
    """
    subject = "File Upload: Failed"
    message = f"The file '{file}' could not be uploaded. Error details: {error_info}"
    return EmailContent(subject, message)


def successful_validation_email(file: str) -> EmailContent:
    """
    Create a subject and message to confirm a file passing validation successfully.
    """
    subject = "Dataset Ingest: Validation Successful"
    message = f"File '{file}' has passed validation."
    return EmailContent(subject, message)


def failed_validation_email(file: str, validation_info: str) -> EmailContent:
    """
    Create a subject and message to report failed validation on dataset ingest.
    """
    subject = "Dataset Ingest: Validation Failed"
    message = f"File '{file}' could not be validated. Validation failure details: {validation_info}"
    return EmailContent(subject, message)


def successful_metadata_submission(dataset_id: str) -> EmailContent:
    """
    Create a subject and message to confirm successful submission of metadata to the Dataset API
    """
    subject = "Dataset Ingest: Metadata Submitted"
    message = f"The metadata for {dataset_id} has been successfully submitted to the Dataset API."
    return EmailContent(subject, message)


def failed_metadata_submission(dataset_id: str, error_info: str) -> EmailContent:
    """
    Create a subject and message to report failed submission of metadata to the Dataset API
    """
    subject = "Dataset Ingest: Metadata Submission Failed"
    message = f"The metadata for {dataset_id} could not be submitted to the Dataset API. Submission failure details: {error_info}"
    return EmailContent(subject, message)

