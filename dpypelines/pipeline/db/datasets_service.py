from typing import Any, Dict, Optional, Tuple

from bson.objectid import ObjectId


from dpypelines.pipeline.db.dataset_statuses_collection import DatasetStatusesCollection
from dpypelines.pipeline.db.datasets_collection import DatasetsCollection
import dpypelines.pipeline.db.db_models as models
from dpypelines.pipeline.db.db_utils import _get_statuses_key_with_dot_notation
from dpypelines.pipeline.db.db_model_factories import DatasetFactory
from dpypelines.pipeline.process_zip_file import S3Object


class DatasetsService:
    """
    Class to manage interactions which update both the datasets and the statuses collections in the state management database.
    """

    def __init__(
        self,
        datasets_collection: DatasetsCollection,
        dataset_statuses_collection: DatasetStatusesCollection,
    ):
        self.datasets_collection = datasets_collection
        self.statuses_collection = dataset_statuses_collection

    def create_new_dataset(
        self,
        dataset_id: str,
        s3_object_key: str,
        filename: str,
        additional_data: Optional[Dict[str, Any]] = None,
        latest_edition_id: Optional[str] = None,
        latest_version_id: Optional[int] = None,
    ) -> Tuple[models.Dataset, ObjectId]:
        """
        Create a new dataset document in the datasets collection.

        :param dataset_id: The dataset_id of the dataset document to create
        :param s3_object_key: The object key of the file received in the S3 ingest bucket
        :param additional_data: Any additional data associated with the event
        :param latest_edition_id: The latest edition id for the given dataset_id
        :param latest_version_id: The latest version id for the given edition_id

        :return: models.Dataset
        """
        # Create new DatasetStatus model and add to statuses collection
        status_model = self.statuses_collection.create_new_status(
            dataset_id=dataset_id,
            s3_object_key=s3_object_key,
            filename=filename,
            edition_id=latest_edition_id,
            version_id=latest_version_id,
            additional_data=additional_data,
        )

        # Create new Dataset model
        dataset_model = DatasetFactory.create_dataset(
            dataset_id=dataset_id,
            created_at=status_model.events[0].timestamp,
            updated_at=status_model.events[0].timestamp,
            latest_edition_id=latest_edition_id,
            latest_version_id=latest_version_id,
            statuses={str(status_model.id): status_model},
        )

        # Create dataset document in datasets collection
        self.datasets_collection.create_dataset(dataset_model=dataset_model)

        return dataset_model, status_model.id

    def create_dataset_if_not_exists(
        self,
        s3_object: S3Object,
    ) -> Tuple[models.Dataset, ObjectId]:
        """
        Check if dataset exists in datasets collection; get dataset document if it exists or create new dataset document if not.

        :param s3_object_key: The object key of the zip file submitted to the S3 ingest bucket.
        :param filename: The filename of the zip file submitted to the S3 ingest bucket
        :param dataset_id: The dataset ID associated with the submitted file.

        :return: Tuple[models.Dataset, ObjectId]
        """
        if self.datasets_collection.dataset_exists(dataset_id=s3_object.dataset_id):
            # Get dataset document from collection and convert to Dataset model
            dataset_model = self.datasets_collection.get_dataset(
                dataset_id=s3_object.dataset_id
            )

            # Create new status document in collection and convert to DatasetStatus model
            status_model = self.statuses_collection.create_new_status(
                dataset_id=s3_object.dataset_id,
                s3_object_key=s3_object.key,
                filename=s3_object.filename,
            )
            dataset_model.statuses[str(status_model.id)] = status_model

            # Update dataset document with new status details and get updated Dataset model
            updated_dataset_model = self.update_dataset_new_status(
                dataset_id=s3_object.dataset_id, new_status=status_model
            )
            return updated_dataset_model, status_model.id
        return self.create_new_dataset(
            dataset_id=s3_object.dataset_id,
            s3_object_key=s3_object.key,
            filename=s3_object.filename,
        )

    def update_dataset_existing_status(
        self,
        dataset_id: str,
        status_oid: ObjectId,
        event: models.DatasetEvent,
        new_status: models.DatasetStatusType,
    ) -> models.Dataset:
        """
        Update dataset details when new events occur during ETL pipeline processing, given that the status field is already populated.

        :param dataset_id: The dataset_id of the dataset document to update
        :param status_oid: The ObjectId of the status to be updated
        :param event: The update event associated with the status update
        :param new_status: The status type associated with the update event

        :return: models.Dataset
        """
        # Get Dataset model for the given dataset_id
        dataset_model = self.datasets_collection.get_dataset(dataset_id=dataset_id)

        # Update status in statuses collection and get updated DatasetStatus model
        try:
            updated_status_model = self.statuses_collection.update_status(
                status_oid=status_oid,
                event=event,
                new_status=new_status,
            )
        except Exception as err:
            raise err

        # Update datasets collection with new status details
        try:
            # Generate update values
            update_values = {}
            update_values[_get_statuses_key_with_dot_notation(status_oid)] = (
                updated_status_model.dict_for_mongodb()
            )
            update_values["updated_at"] = updated_status_model.updated_at.isoformat()

            # Update dataset in datasets collection and convert to Dataset model
            updated_dataset_model = self.datasets_collection.update_dataset(
                dataset_model, update_values
            )
        except Exception as err:
            raise err

        return updated_dataset_model

    def update_dataset_new_status(
        self, dataset_id: str, new_status: models.DatasetStatus
    ) -> models.Dataset:
        """
        Update an existing dataset document with a new status when a new run of the ETL pipeline begins.

        :param dataset_id: The dataset_id of the dataset document to update
        :param new_status: the DatasetStatus model to be added to dataset.statuses in the dataset document

        :return: models.Dataset
        """
        # Get Dataset model for the given dataset_id
        dataset_model = self.datasets_collection.get_dataset(dataset_id=dataset_id)

        # Generate update values
        update_values = {
            _get_statuses_key_with_dot_notation(
                new_status.id
            ): new_status.dict_for_mongodb()
        }
        update_values["updated_at"] = new_status.updated_at.isoformat()

        # Update the dataset document with the new status
        self.datasets_collection.update_dataset(
            dataset_model=dataset_model, update_values=update_values
        )

        # Get the updated dataset model
        updated_dataset_model = self.datasets_collection.get_dataset(
            dataset_id=dataset_id
        )

        return updated_dataset_model
