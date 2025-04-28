import sqlite3
from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult


from pathlib import Path


class SQLiteValidator(FileFormatValidator):
    def __init__(self):
        super().__init__(["csdb", "sqlite", "db", "db3"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        """
        Validate that the file is a valid SQLite3 (or CSDB) file
        """

        "Needed because an empty file is still a valid sqlite3 DB (!) and there's no guarantees that the file was validated this way before"
        if file_path.stat().st_size == 0:
            return self._generate_error(file_path, "File is empty")

        try:
            conn = sqlite3.connect(f"file:{file_path}?mode=ro", uri=True)
            cursor = conn.cursor()

            result = self._verify_database_file(file_path, cursor)

            cursor.close()
            conn.close()

            return result
        except Exception as e:
            return self._generate_error(file_path, str(e))

    def _verify_database_file(
        self, file_path: Path, cursor: sqlite3.Cursor
    ) -> ValidationResult:
        try:
            cursor.execute("PRAGMA integrity_check")
            return self._generate_success(file_path)
        except sqlite3.DatabaseError as e:
            return self._generate_error(file_path, str(e))
