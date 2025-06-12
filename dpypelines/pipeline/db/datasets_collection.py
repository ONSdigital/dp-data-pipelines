from typing import Any, Dict, List

from bson.objectid import ObjectId

from dpytools.db.documentdb_client import DocumentDBClient
from dpypelines.pipeline.db.base_collection import BaseCollection
import dpypelines.pipeline.db.db_models as models
from dpypelines.pipeline.db.db_utils import (
    _convert_status_ids_and_validate,
)
from dpypelines.pipeline.errors import (
    DocumentNotFoundException,
    DocumentNotUpdatedException,
    DocumentNotCreatedException,
)


class DatasetsCollection(BaseCollection):
    """
    Class to manage interactions with the datasets collection in the state management database.
    """

    def __init__(self, client: DocumentDBClient):
        super().__init__(client, "datasets")

    def initialise_collection(self):
        self.__collection = super().initialise_collection()
        return self.__collection

    def get_dataset(self, dataset_id: str) -> models.Dataset:
        """
        Get dataset from `datasets` collection and convert to Dataset model.

        :param dataset_id: The dataset ID of the dataset document to retrieve

        :return: models.Dataset
        """
        # Get dataset document for the given dataset_id
        dataset_document = self.__collection.read_one_document(
            {"dataset_id": dataset_id}
        )

        if not dataset_document:
            raise DocumentNotFoundException(
                message="Dataset document not found in collection",
                query_filter={"dataset_id": dataset_id},
            )

        # Convert status and event ID values to ObjectIds and generate model
        dataset_model = _convert_status_ids_and_validate(dataset_document)

        return dataset_model

    def get_all_datasets(self) -> List[models.Dataset]:
        """
        Get all datasets from the datasets collection and convert to Dataset models.

        :return: List[models.Dataset]
        """
        # Get all documents in the datasets collection
        dataset_documents = self.__collection.read_many_documents()

        if not dataset_documents:
            raise DocumentNotFoundException(
                "No dataset documents found in collection", query_filter={}
            )

        # Convert status and event ID values to ObjectIds and generate models
        all_dataset_models = [
            _convert_status_ids_and_validate(document) for document in dataset_documents
        ]

        return all_dataset_models

    def create_dataset(self, dataset_model: models.Dataset) -> None:
        """
        Create a new document in the datasets collection

        :param dataset_model: the Dataset model to add to the collection
        """
        # Convert Dataset model to dict
        dataset_dict = dataset_model.dict_for_mongodb()
        dataset_dict["_id"] = ObjectId(dataset_dict["id"])

        # Add new dataset document to collection
        result = self.__collection.create_one_document(dataset_dict)

        if not result.acknowledged:
            raise DocumentNotCreatedException(
                message="Dataset document not created in collection",
                data={"dataset_id": dataset_model.dataset_id},
            )

    def dataset_exists(self, dataset_id: str):
        dataset = self.__collection.read_one_document({"dataset_id": dataset_id})
        if dataset is not None:
            return True
        return False

    def update_dataset(
        self, dataset_model: models.Dataset, update_values: Dict[str, Any]
    ) -> models.Dataset:
        """
        Update dataset document with the given update values.

        :param dataset_model: the Dataset model to be updated in the collection
        :param update_values: The values to be updated in the given dataset document

        :return: models.Dataset
        """
        # Update dataset document with new values
        result = self.__collection.update_one_document(
            {"_id": dataset_model.id}, update_values
        )

        if not result.acknowledged:
            raise DocumentNotUpdatedException(
                message="Dataset document not updated",
                data={"dataset_id": dataset_model.dataset_id},
            )

        # Get updated dataset document and convert to Dataset model
        updated_dataset = self.get_dataset(dataset_model.dataset_id)

        return updated_dataset
