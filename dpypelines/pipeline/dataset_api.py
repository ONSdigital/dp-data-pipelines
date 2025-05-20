from dpytools.http.api.dataset_api_service import DatasetAPIService
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.errors import (
    DatasetNotFoundException,
)
from dpypelines.pipeline.errors.dataset_type_exception import DatasetTypeException
from dpypelines.pipeline.models.metadata_models import DatasetVersion, Metadata
from dpytools.http.api.models.dataset import DatasetType

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
    return dataset_type_is_static(dataset_id, dataset_api_service)


def dataset_type_is_static(
    dataset_id: str, dataset_api_service: DatasetAPIService
) -> bool:
    """
    Return a boolean specifying whether the dataset type is `static` (True) or not (False).

    If either `get_dataset_id_path_response` or `get_current_dataset_type` fail to generate the required values, raise an error.
    """
    dataset = dataset_api_service.datasets.get_dataset(dataset_id)
    if not dataset:
        raise DatasetNotFoundException(
            "Dataset not found",
            dataset_path=dataset_api_service.datasets._build_full_url(dataset_id),
        )

    if dataset.current is None:
        raise KeyError("'current' not found in dataset_result keys")

    dataset_type = dataset.current.type

    if dataset_type is None:
        raise DatasetTypeException(
            f"No dataset type found in dataset {dataset_id}", dataset_type
        )

    return dataset_type == DatasetType.STATIC or dataset_type == "static"
