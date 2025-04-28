from dpypelines.pipeline.validation.file_format_validator import FileFormatValidator
from dpypelines.pipeline.validation.models import ValidationResult


from pathlib import Path


class XMLValidator(FileFormatValidator):
    def __init__(self):
        super().__init__(["xml"])

    def validate_file_format(self, file_path: Path) -> ValidationResult:
        try:
            import xml.etree.ElementTree as ET

            # Try to parse XML using ElementTree
            tree = ET.parse(file_path)
            tree.getroot()

            return self._generate_success(file_path)
        except ET.ParseError as e:
            return self._generate_error(file_path, f"XML parse error: {str(e)}")
        except Exception as e:
            return self._generate_error(file_path, f"XML validation error: {str(e)}")
