from .csv_validator import CSVValidator
from .excel_validator import ExcelValidator
from .json_validator import JSONValidator
from .text_validator import TextValidator
from .xml_validator import XMLValidator
from .sqlite_validator import SQLiteValidator
from .file_format_validator import FileFormatValidator


def get_file_validators() -> list[FileFormatValidator]:
    return [
        CSVValidator(),
        ExcelValidator(),
        JSONValidator(),
        TextValidator(),
        XMLValidator(),
        SQLiteValidator(),
    ]
