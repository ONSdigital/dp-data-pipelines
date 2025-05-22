from typing import Any, Dict
from dpytools.logging.data_exception import DataException


class DocumentNotFoundException(DataException):
    def __init__(self, message: str, query_filter: Dict[str, Any], *args: object):
        data = {"query_filter": query_filter}
        super().__init__(message, data=data, *args)
