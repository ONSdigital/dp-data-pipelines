from pathlib import Path
from dpypelines.pipeline.validation.sqlite_validator import SQLiteValidator
from tests.helpers.generators.data_file_generators import generate_csdb
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)


class TestSQLiteValidator(BaseTestFileValidator):
    file_format: str = "csdb"
    validator_type = SQLiteValidator

    def generate_test_file(self, file_path: Path):
        generate_csdb(file_path)

    def test_validator_passes_valid_file(self):
        """Test that sqlite files are validated with the sqlite validator."""
        self._test_validator_passes_valid_file()
        assert True

    def test_validator_errors_empty_file(self):
        """Test that sqlite files are validated with the sqlite validator."""
        self._test_validator_errors_empty_file()

    def test_handles_sqlite_error(self):
        file_path = self._get_test_file_path()

        with open(file_path, "w") as f:
            f.write("this is not a sqlite file")
