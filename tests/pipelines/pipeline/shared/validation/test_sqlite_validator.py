from pathlib import Path
import sqlite3
from dpypelines.pipeline.validation.sqlite_validator import SQLiteValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)


class TestSQLiteValidator(BaseTestFileValidator):
    file_format: str = "csdb"
    validator_type = SQLiteValidator

    def generate_test_file(self, file_path: Path):
        conn = sqlite3.connect(str(file_path))
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE dummy_table(id)")
        conn.close()

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
