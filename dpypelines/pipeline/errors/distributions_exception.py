from dpytools.logging.data_exception import DataException


class DistributionsException(DataException):
    def __init__(self, message: str, distributions: list, *args):
        data = {"distributions": distributions}
        super().__init__(message, data=data, *args)()
