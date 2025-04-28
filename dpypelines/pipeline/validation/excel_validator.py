from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult


import pandas as pd


from pathlib import Path


class ExcelValidator(FileFormatValidator):
    _xls_headers = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
    _xlsx_headers = b"PK\x03\x04"

    def __init__(self):
        super().__init__(["xls", "xlsx"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        try:
            return self._try_read_with_pandas(file_path)
        except ImportError:
            return self._check_file_headers(file_path)
        except Exception as e:
            return self._generate_error(
                file_path, f"Excel file validation error: {str(e)}"
            )

    def _try_read_with_pandas(self, file_path: Path) -> ValidationResult:
        with pd.ExcelFile(file_path) as xl:
            sheet_names = xl.sheet_names
            if not sheet_names:
                return self._generate_error(file_path, "Excel file contains no sheets")

            try:
                first_sheet = sheet_names[0]
                pd.read_excel(xl, sheet_name=first_sheet, nrows=1)
                return self._generate_success(file_path)
            except Exception as sheet_error:
                return self._generate_error(
                    file_path,
                    f"Could open xls/xlsx file but failed to read sheet data: {str(sheet_error)}",
                )

    def _check_file_headers(self, file_path: Path) -> ValidationResult:
        """
        Fallback incase of Pandas issue. Check file headers start with the relevant bytes used for the file types
        """
        with open(file_path, "rb") as f:
            header = f.read(8)
            is_xls = header.startswith(self._xls_headers)
            is_xlsx = header.startswith(self._xlsx_headers)

            is_valid = is_xls or is_xlsx

            return (
                self._generate_success(file_path)
                if is_valid
                else self._generate_error(file_path, "Invalid Excel file signature")
            )
