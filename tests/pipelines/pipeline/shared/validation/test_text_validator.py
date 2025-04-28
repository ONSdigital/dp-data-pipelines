from dpypelines.pipeline.validation.text_validator import TextValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)
from pathlib import Path


class TestXMLValidator(BaseTestFileValidator):
    file_format: str = "txt"
    validator_type = TextValidator

    def generate_test_file(self, file_path: Path, content: str = """."""):
        with open(file_path, "w") as textfile:
            textfile.write(content)

    def test_validator_passes_valid_file(self):
        self._test_validator_passes_valid_file()

    def test_validator_errors_empty_file(self):
        self._test_validator_errors_empty_file()

    def test_validator_errors_file_with_only_whitespace(self):
        file_path = self._get_test_file_path()

        content = """
        
        
        
        """

        self.generate_test_file(file_path, content)

        validator = self.validator_type()
        result = validator.validate_file_format(file_path)

        self.validate_error(result)
