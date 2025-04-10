from typing import Optional

from dpytools.logging.data_exception import DataException


class DatasetTypeException(DataException):
    def __init__(self, message: str, dataset_type: Optional[str], *args: object):
        data = {"dataset_type": dataset_type}
        super().__init__(message, data=data, *args)
