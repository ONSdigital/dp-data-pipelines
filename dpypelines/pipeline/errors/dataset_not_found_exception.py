from dpytools.logging.data_exception import DataException


class DatasetNotFoundException(DataException):
    def __init__(self, message: str, dataset_path: str, *args: object):
        data = {"dataset_path": dataset_path}
        super().__init__(message, data=data, *args)
