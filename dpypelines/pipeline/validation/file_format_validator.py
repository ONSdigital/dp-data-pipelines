from abc import ABC, abstractmethod
from pathlib import Path
from dpypelines.pipeline.validation.models import ValidationResult


class FileFormatValidator(ABC):
    def __init__(self, file_formats: list[str]):
        if not file_formats or len(file_formats) == 0:
            raise ValueError("file_formats cannot be empty")

        self.file_formats = file_formats

    def accepts_file(self, file_path: Path) -> bool:
        """
        Check whether the file specified matches of the file formats the concrete class validates against.
        """
        extension = self._get_file_extension(file_path)

        return extension in self.file_formats

    @abstractmethod
    def validate_file_format(self, file_path: Path) -> ValidationResult:
        pass

    def _get_file_extension(self, file_path: Path) -> str:
        return file_path.suffix.lower().strip(".")

    def _generate_success(self, file_path: Path) -> ValidationResult:
        return ValidationResult(True, self._get_file_extension(file_path))

    def _generate_error(self, file_path: Path, error: str) -> ValidationResult:
        return ValidationResult(False, self._get_file_extension(file_path), error)
