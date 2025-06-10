from dpytools.http.api import DatasetAPIService, Dataset, GetDatasetResponse

from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.errors import (
    DatasetNotFoundException,
)
from dpypelines.pipeline.errors.dataset_not_found_exception import CurrentDatasetNotFoundException
from dpypelines.pipeline.models.metadata_models import DatasetVersion, Metadata

logger = DpLogger("data-ingress-pipelines")


def validate_and_upload_metadata(
    metadata: Metadata,
    dataset_api_service: DatasetAPIService,
) -> bool:
    """
    Verify that the required conditions are met for the metadata to be uploaded the the Dataset API and submit the `POST` request with the required parameters.
    """
    if not is_valid_dataset(metadata.dataset_id, dataset_api_service):
        return False

    return upload_metadata(metadata, dataset_api_service)


def upload_metadata(
    metadata: Metadata,
    dataset_api_service: DatasetAPIService,
) -> bool:
    """
    Send a `POST` request to the `datasets/{dataset_id}/editions/{edition_id}/versions` endpoint to submit the metadata.
    """
    request_body = DatasetVersion(**metadata.model_dump()).model_dump()
    post_metadata_response = dataset_api_service.versions.create_version(
        request_body, dataset_id=metadata.dataset_id, edition_id=metadata.edition
    )
    post_metadata_response.raise_for_status()
    logger.info(
        "Metadata submitted to Dataset API endpoint",
        data={
            "dataset_api_endpoint": dataset_api_service.versions._build_full_url(
                dataset_id=metadata.dataset_id, edition_id=metadata.edition
            )
        },
    )
    return True


def is_valid_dataset(dataset_id: str, dataset_api_service: DatasetAPIService) -> bool:
    """
    Verify that the `datasets/{dataset_id}/editions/{edition_id}/versions` endpoint exists and that the dataset type is `static`.
    """
    dataset_response = get_dataset(dataset_id, dataset_api_service)
    
    validate_dataset_has_current_version(dataset_id=dataset_id, dataset_response=dataset_response)
    
    return True

def validate_dataset_has_current_version(dataset_id: str, dataset_response: GetDatasetResponse):
    if dataset_response.current is None:
        raise CurrentDatasetNotFoundException(dataset_id=dataset_id)

def get_dataset(dataset_id: str, dataset_api_service: DatasetAPIService):
    dataset = dataset_api_service.datasets.get_dataset(dataset_id)
    if not dataset:
        raise DatasetNotFoundException(
            dataset_id=dataset_id,
            dataset_path=dataset_api_service.datasets._build_full_url(dataset_id),
        )
    return dataset