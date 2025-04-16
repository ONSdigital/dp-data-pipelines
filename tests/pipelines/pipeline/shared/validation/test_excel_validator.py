import pandas as pd
from dpypelines.pipeline.validation.excel_validator import ExcelValidator
from tests.pipelines.pipeline.shared.validation.base_test_file_validator import (
    BaseTestFileValidator,
)
from pathlib import Path


class BaseTestExcelValidator(BaseTestFileValidator):
    validator_type = ExcelValidator

    def generate_test_file(self, file_path: Path):
        data = {"id": [1, 2], "name": ["item1", "item2"], "value": [42.5, 13.7]}
        df = pd.DataFrame(data)

        df.to_excel(file_path, index=False)


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
