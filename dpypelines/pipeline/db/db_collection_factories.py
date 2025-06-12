from dpytools.db.documentdb_client import DocumentDBClient
from dpypelines.pipeline.db.dataset_statuses_collection import DatasetStatusesCollection
from dpypelines.pipeline.db.datasets_collection import DatasetsCollection
from dpypelines.pipeline.db.datasets_service import DatasetsService


class DatasetsServiceFactory:
    @staticmethod
    def create_db_datasets_service(client: DocumentDBClient) -> DatasetsService:
        """
        Create a new DatasetsService object with datasets and statuses collections.

        :param client: The DocumentDBClient for connecting and interacting with the database.

        :return: dpypelines.db.DatasetsService
        """
        datasets_collection = DatasetsCollection(client)
        datasets_collection.initialise_collection()
        dataset_statuses_collection = DatasetStatusesCollection(client)
        dataset_statuses_collection.initialise_collection()
        return DatasetsService(datasets_collection, dataset_statuses_collection)
