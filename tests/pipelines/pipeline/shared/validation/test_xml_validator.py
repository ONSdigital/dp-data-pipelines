from dpypelines.pipeline.validation.xml_validator import XMLValidator
from tests.helpers.generators.data_file_generators import generate_xml
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)
from pathlib import Path


class TestXMLValidator(BaseTestFileValidator):
    file_format: str = "xml"
    validator_type = XMLValidator

    def generate_test_file(self, file_path: Path):
        generate_xml(file_path)

    def test_validator_passes_valid_file(self):
        self._test_validator_passes_valid_file()

    def test_validator_errors_empty_file(self):
        self._test_validator_errors_empty_file()
