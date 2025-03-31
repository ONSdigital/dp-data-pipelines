from pathlib import Path
from typing import Dict

from dpytools.logging.data_exception import DataException


class ValidationException(DataException):
    def __init__(
        self, message: str, files_dir: Path, validation_results: Dict, *args: object
    ):
        data = {
            "files_dir": str(files_dir),
            "validation_results": validation_results,
        }

        super().__init__(message, data=data, *args)
