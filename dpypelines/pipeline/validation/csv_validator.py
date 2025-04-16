from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult


import csv
from pathlib import Path


class CSVValidator(FileFormatValidator):
    def __init__(self):
        super().__init__(["csv"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        """
        Validate that the file has at least one row with one column in it.
        """
        try:
            with open(file_path, "r", newline="", encoding="utf-8") as f:
                csv_reader = csv.reader(f)
                header = next(csv_reader, None)

                if not header:
                    return self._generate_error(
                        file_path, "No header row found in CSV file"
                    )

                if len(header) == 0:
                    return self._generate_error(
                        file_path, "No columns found in CSV file"
                    )

                return self._generate_success(file_path)
        except Exception as e:
            return self._generate_error(file_path, f"CSV validation error: {str(e)}")
