from dpytools.logging.data_exception import DataException


class DatasetAPIRequestCreationException(DataException):
    def __init__(self, message: str, metadata: dict, *args: object):
        data = {"metadata": metadata}
        super().__init__(message, data=data, *args)
