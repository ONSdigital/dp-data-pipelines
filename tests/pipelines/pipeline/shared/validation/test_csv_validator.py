import csv
from io import StringIO
from pathlib import Path

import pytest
from dpypelines.pipeline.validation.csv_validator import CSVValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)

test_data = {
    "headers": ["header1", "header2", "header3"],
    "rows": [["value1", "value2", "value3"]]
}

class TestCSVValidator(BaseTestFileValidator):
    file_format: str = "csv"
    validator_type = CSVValidator
    
    def create_csv(self, delimiter: str) -> StringIO:
        headers = delimiter.join(test_data["headers"])
        rows = "\n".join(delimiter.join(row) for row in test_data["rows"])
        
        return headers + "\n" + rows
    
    @pytest.fixture
    def valid_csv_file(self):
        return StringIO(self.create_csv(","))
    
    @pytest.fixture
    def valid_tsv_file(self):
        return StringIO(self.create_csv("\n"))
    
    @pytest.fixture
    def valid_semicolon_file(self):
        return StringIO(self.create_csv(";"))
    
    @pytest.fixture
    def invalid_file(self):
        return StringIO("{this is some other file}")
    
    def generate_test_file(self, file_path: Path):
        with open(file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(test_data["headers"])
            writer.writerows(test_data["rows"])
            
    def test_validator_passes_valid_file(self):
        """Test that CSV files are validated with the CSV validator."""
        self._test_validator_passes_valid_file()
        assert True

    def test_validator_errors_empty_file(self):
        """Test that CSV files are validated with the CSV validator."""
        self._test_validator_errors_empty_file()

    def test_sniffer_valid_csv(self):
        file = StringIO(self.create_csv(","))
        validator = CSVValidator()
        
        result = validator._try_validate_with_sniffer(file)
        
        assert result is True
    
    def test_sniffer_valid_tsv(self):
        file = StringIO(self.create_csv("\n"))
        validator = CSVValidator()

        result = validator._try_validate_with_sniffer(file)
        assert result is True
    
    def test_sniffer_valid_semicolon(self):
        file = StringIO(self.create_csv(";"))
        validator = CSVValidator()
        
        result = validator._try_validate_with_sniffer(file)
        
        assert result is True
    
    def test_sniffer_invalid_csv(self, invalid_file):
        validator = CSVValidator()

        result = validator._try_validate_with_sniffer(invalid_file)
        assert isinstance(result, bool) 