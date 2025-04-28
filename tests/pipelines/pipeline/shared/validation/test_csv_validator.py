import csv
from pathlib import Path
from dpypelines.pipeline.validation.csv_validator import CSVValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)


class TestCSVValidator(BaseTestFileValidator):
    file_format: str = "csv"
    validator_type = CSVValidator

    def generate_test_file(self, file_path: Path):
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["id", "name", "value"])

    def test_validator_passes_valid_file(self):
        """Test that CSV files are validated with the CSV validator."""
        self._test_validator_passes_valid_file()
        assert True

    def test_validator_errors_empty_file(self):
        """Test that CSV files are validated with the CSV validator."""
        self._test_validator_errors_empty_file()
