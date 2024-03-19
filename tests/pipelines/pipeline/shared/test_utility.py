import pytest
import os

from dpypelines.pipeline.shared.utility import enrich_online
from dpypelines.pipeline.shared.message import unexpected_error


@enrich_online
def very_much_expected_error(msg: str, error: Exception) -> str:
    """
    We've caught an unexpected error. Make a sensible message explaining
    the problem.
    """
    error_type = error.__class__.__name__
    message = f"""
        {msg}

        Error type: {error_type}
        Error: {error}
    """

    return message


def test_enrich_message():
    os.environ["ENRICH_MESSAGE"] = "Please work"
    

    error = Exception("Some file is missing.")
    expected_message = """
        Something went wrong

        Error type: Exception
        Error: Some file is missing.
    """
    expected_message = expected_message + " " + "Please work" + "\n notification-source"

    assert very_much_expected_error("Something went wrong", error) == expected_message