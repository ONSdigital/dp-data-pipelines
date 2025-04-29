from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult


import tempfile
from pathlib import Path
from typing import Optional, Type


class BaseTestFileValidator:
    file_format: str
    validator_type: Type[FileFormatValidator]

    def setup_method(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)

    def teardown_method(self):
        self.temp_dir.cleanup()

    def generate_test_file(self, file_path: Path):
        raise NotImplementedError()

    def _create_file_name(self):
        return f"test.{self.file_format}"

    def test_accepts_file_type(self):
        file_path = self._get_test_file_path()

        validator = self.validator_type()

        accepts = validator.accepts_file(file_path)

        assert accepts

    def _test_validator_passes_valid_file(self):
        file_path = self._get_test_file_path()

        self.generate_test_file(file_path)

        validator = self.validator_type()
        result = validator.validate_file_format(file_path)

        self.validate_success(result)

    def _test_validator_errors_empty_file(self):
        file_path = self._generate_empty_file()
        validator = self.validator_type()

        result = validator.validate_file_format(file_path)

        self.validate_error(result)

    def _generate_empty_file(self):
        file_path = self._get_test_file_path()
        file_path.touch()
        return file_path

    def test_validator_errors_invalid_file(self):
        file_path = self._get_test_file_path()

        with open(file_path, "w") as f:
            f.write(
                '\x00\nt\n"dsaasd,"\n\theader1#header2#header3\nvalue1#value2#value3'
            )

        validator = self.validator_type()

        result = validator.validate_file_format(file_path)

        self.validate_error(result)

    def validate_error(self, result: ValidationResult, error: Optional[str] = None):
        assert not result.valid
        assert result.error is not None if error is None else error
        assert result.format == self._file_type()

    def validate_success(self, result: ValidationResult):
        assert result.valid
        assert not result.error
        assert result.format == self._file_type()

    def _get_test_file_path(self) -> Path:
        return self.test_dir / self._create_file_name()

    def _file_type(self):
        return self.file_format
