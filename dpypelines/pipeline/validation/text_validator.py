from pathlib import Path
from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult


class TextValidator(FileFormatValidator):
    def __init__(self):
        super().__init__(["txt"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        """
        Validate plain text file by checking if it can be read as text.

        Args:
            file_path: Path to the text file

        Returns:
            Validation result dictionary
        """
        try:
            return (
                self._generate_success(file_path)
                if self._file_has_any_line(file_path)
                else self._generate_error(file_path, "All lines are empty")
            )
        except UnicodeDecodeError:
            return self._generate_error(
                file_path, "File contains non-text (binary) data"
            )
        except Exception as e:
            return self._generate_error(
                file_path, f"Text file validation error: {str(e)}"
            )

    def _file_has_any_line(self, file_path: Path) -> bool:
        """
        Check if the file contains at least one non-empty line.

        Args:
            file_path: The Path of the file to check

        Returns:
            boolean indicating success (at least one non-empty line) or failure (all lines empty)

        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return any(len(line.strip()) > 0 for line in f)
            return False
        except Exception:
            return False
