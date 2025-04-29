from dpypelines.pipeline.validation.excel_validator import ExcelValidator
from tests.helpers.generators.data_file_generators import generate_excel
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)
from pathlib import Path


class BaseTestExcelValidator(BaseTestFileValidator):
    validator_type = ExcelValidator

    def generate_test_file(self, file_path: Path):
        generate_excel(file_path)


class TestXLSValidator(BaseTestExcelValidator):
    file_format = "xls"

    def test_validator_passes_valid_file(self):
        self._test_validator_passes_valid_file()

    def test_validator_errors_empty_file(self):
        self._test_validator_errors_empty_file()


class TestXLSXValidator(BaseTestExcelValidator):
    file_format = "xlsx"

    def test_validator_passes_valid_file(self):
        self._test_validator_passes_valid_file()

    def test_validator_errors_empty_file(self):
        self._test_validator_errors_empty_file()
