from io import TextIOWrapper
from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult

import csv
from pathlib import Path


class CSVValidator(FileFormatValidator):
    def __init__(self):
        super().__init__(["csv", "tsv"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        """
        Validate that the file is a valid CSV.

        Args:
            file_path: Path object representing the data file to validate

        Returns:
            ValidationResult with the validation result data
        """
        try:
            if file_path.stat().st_size == 0:
                return self._generate_error(
                    file_path, f"File '{str(file_path)}' is empty"
                )

            with open(file_path, "r", newline="", encoding="utf-8") as file:
                valid_csv = self._try_validate_with_sniffer(file)

                return (
                    self._generate_success(file_path)
                    if valid_csv
                    else self._generate_error(
                        file_path, f"Could not parse {file_path} as a valid CSV"
                    )
                )
        except Exception as e:
            return self._generate_error(file_path, f"CSV validation error: {str(e)}")

    def _try_validate_with_sniffer(self, file: TextIOWrapper) -> bool:
        """
        Try to "sniff" the format of the file, and then validate based on the sniffed dialect

        Args:
            file: The open file to try to validate

        Returns:
            bool: True == valid, False == invalid
        """
        try:
            # Grab a sample of the CSV for format detection.
            csv_test_bytes = file.read(1024)
            file.seek(0)

            # Rewind for future reading if needed
            has_header = csv.Sniffer().has_header(csv_test_bytes)

            # Check to see if there's a header in the file.
            dialect = csv.Sniffer().sniff(csv_test_bytes)

            # Check what kind of csv/tsv file we have.
            inputreader = csv.reader(file, dialect)
            if has_header:
                # Read next to be sure
                next(inputreader)
            return True
        except csv.Error:
            return False
