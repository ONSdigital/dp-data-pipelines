import json
from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult
from pathlib import Path


class JSONValidator(FileFormatValidator):
    def __init__(self):
        super().__init__(["json"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        """
        Validate that the file is valid JSON
        """
        try:
            with open(file_path, "r") as file:
                json.load(file)
            return self._generate_success(file_path)
        except json.JSONDecodeError:
            return self._generate_error(file_path, "File is not valid JSON")
        except FileNotFoundError:
            return self._generate_error(file_path, f"JSON file {file_path} not found")
        except PermissionError:
            return self._generate_error(
                file_path,
                f"Permission denied when trying to read JSON file '{file_path}'.",
            )
        except Exception as e:
            return self._generate_error(
                file_path, f"Unhandled JSON validation error: {str(e)}"
            )

    def _try_load_json_file(self, file_path: Path) -> ValidationResult:
        """
        Read file and load as JSON.
        """
        with open(file_path, "r") as file:
            json.load(file)
