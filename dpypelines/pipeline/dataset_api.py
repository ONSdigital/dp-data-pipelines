import json

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.config import JobConfiguration
from dpypelines.pipeline.errors import (
    DatasetNotFoundException,
    DatasetTypeException,
)
from dpypelines.pipeline.models import DatasetVersion, Metadata

logger = DpLogger("data-ingress-pipelines")


def validate_and_upload_metadata(metadata: Metadata) -> bool:
    """
    Verify that the required conditions are met for the metadata to be uploaded the the Dataset API and submit the `POST` request with the required parameters.
    """
    dataset_api_url = JobConfiguration().dataset_api_url
    if not dataset_api_url:
        msg = "Required variable not set: DATASET_API_URL"
        raise EnvironmentError(msg)

    dataset_api_client = DatasetAPIClient(
        dataset_api_url, metadata.dataset_id, metadata.edition
    )

    if not is_valid_dataset(dataset_api_client):
        raise ValueError("Invalid dataset")

    return upload_metadata(metadata, dataset_api_client)


def upload_metadata(metadata: Metadata, dataset_api_client: DatasetAPIClient) -> bool:
    """
    Send a `POST` request to the `datasets/{dataset_id}/editions/{edition_id}/versions` endpoint to submit the metadata.
    """
    request_body = DatasetVersion(**metadata.model_dump()).model_dump()
    post_metadata_response = dataset_api_client.post_json(request_body)
    post_metadata_response.raise_for_status()
    logger.info(
        "Metadata submitted to Dataset API endpoint",
        data={"dataset_api_endpoint": dataset_api_client.full_url},
    )
    return True


def is_valid_dataset(dataset_api_client: DatasetAPIClient) -> bool:
    """
    Verify that the `datasets/{dataset_id}/editions/{edition_id}/versions` endpoint exists and that the dataset type is `static`.
    """
    return dataset_versions_path_exists(dataset_api_client) and dataset_type_is_static(
        dataset_api_client
    )


def dataset_versions_path_exists(dataset_api_client: DatasetAPIClient) -> bool:
    """
    Send a `GET` request to the `datasets/{dataset_id}/editions/{edition_id}/versions` endpoint to confirm existence.
    """
    get_full_path_response = dataset_api_client.get_path()
    get_full_path_response.raise_for_status()
    logger.info(
        "Dataset API endpoint exists",
        data={"dataset_api_endpoint": dataset_api_client.full_url},
    )
    return True


def dataset_type_is_static(dataset_api_client: DatasetAPIClient) -> bool:
    """
    Return a boolean specifying whether the dataset type is `static` (True) or not (False).

    If either `get_dataset_id_path_response` or `get_current_dataset_type` fail to generate the required values, raise an error.
    """
    dataset_id_path_response = get_dataset_id_path_response(dataset_api_client)
    if not dataset_id_path_response:
        raise DatasetNotFoundException(
            "Dataset not found", dataset_path=dataset_api_client.dataset_path
        )

    dataset_type = get_current_dataset_type(dataset_id_path_response)
    if not dataset_type:
        raise DatasetTypeException("Dataset type error", dataset_type=str(dataset_type))

    return dataset_type == "static"


def get_dataset_id_path_response(dataset_api_client: DatasetAPIClient) -> dict:
    """
    Send a `GET` request to the `datasets/{dataset-id}` endpoint and load the result text as a dict.
    """
    get_dataset_result = dataset_api_client.get(
        f"{dataset_api_client.dataset_api_url}/{dataset_api_client.dataset_path}",
        headers=dataset_api_client.token_auth.get_auth_header(),
    )
    if get_dataset_result.status_code != 200:
        get_dataset_result.raise_for_status()
        return

    return json.loads(get_dataset_result.text)


def get_current_dataset_type(dataset_result: dict) -> str:
    """
    Check that the result returned by `get_dataset_id_path_response` contains a `current` object, and get the value of `type` in this object.
    """
    current_dataset = dataset_result.get("current", None)
    if current_dataset:
        return current_dataset.get("type", None)
    else:
        raise KeyError("'current' not found in dataset_result keys")
