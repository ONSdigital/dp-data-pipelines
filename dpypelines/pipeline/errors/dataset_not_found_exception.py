from dpytools.logging.data_exception import DataException


class DatasetNotFoundException(DataException):
    def __init__(self, dataset_id: str, dataset_path: str, *args: object):
        self.dataset_id = dataset_id
        data = {"dataset_path": dataset_path, dataset_id: dataset_id}
        super().__init__(f"Dataset {dataset_id} not found", data=data, *args)


class CurrentDatasetNotFoundException(DataException):
    def __init__(self, dataset_id: str, *args: object):
        self.dataset_id = dataset_id
        self.data = {"dataset_id": dataset_id}
        super().__init__(
            f"Dataset {dataset_id} does not have a 'current' version",
            data=self.data,
            *args,
        )
