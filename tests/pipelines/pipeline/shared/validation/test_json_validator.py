from dpypelines.pipeline.validation.json_validator import JSONValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)
import json
from pathlib import Path


class TestJSONValidator(BaseTestFileValidator):
    file_format: str = "json"
    validator_type = JSONValidator

    def generate_test_file(self, file_path: Path):
        data = {}
        with open(file_path, "w") as jsonfile:
            json.dump(data, jsonfile, indent=2)

    def test_validator_passes_valid_file(self):
        """Test that CSV files are validated with the CSV validator."""
        self._test_validator_passes_valid_file()
        assert True

    def test_validator_errors_empty_file(self):
        """Test that CSV files are validated with the CSV validator."""
        self._test_validator_errors_empty_file()
