from typing import Any, Dict
from dpytools.logging.data_exception import DataException


class DocumentNotUpdatedException(DataException):
    def __init__(self, message: str, data: Dict[str, Any], *args: object):
        super().__init__(message, data=data, *args)
