import json
from datetime import datetime

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.errors import (
    DatasetAPIRequestCreationException,
    DatasetNotFoundException,
    DatasetTypeException,
)
from dpypelines.pipeline.models import Metadata

logger = DpLogger("data-ingress-pipelines")


def get_post_request_values_from_metadata(metadata: Metadata):
    """
    Generate the required path values and request body to submit to the Dataset API. This will be submitted as a POST request to the endpoint `/datasets/{dataset_path}/editions/{edition_path}/versions`
    """
    try:
        dataset_path = metadata.dataset_id
        edition_path = metadata.edition

        request_body = {
            "edition_title": metadata.edition_title,
            "quality_designation": metadata.quality_designation,
            # 2885 metadata standards document says distribution.byte_size should come from querying the files API?
            "distributions": [
                distribution.model_dump() for distribution in metadata.distributions
            ],
            "usage_notes": [
                usage_note.model_dump() for usage_note in metadata.usage_notes
            ],
            "alerts": [alert.model_dump() for alert in metadata.alerts],
            # 2885 Should release_date be the timestamp of when the pipeline runs, or is it determined by a date specified in the publishing schedule?
            "release_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        return dataset_path, edition_path, request_body
    except Exception as err:
        raise DatasetAPIRequestCreationException(
            "Error getting POST request values from metadata", metadata=metadata
        ) from err


def get_dataset_api_response(dataset_api_client: DatasetAPIClient) -> dict:
    get_dataset_result = dataset_api_client.get(
        f"{dataset_api_client.dataset_api_url}/{dataset_api_client.dataset_path}",
        headers=dataset_api_client.token_auth.get_auth_header(),
    )
    if get_dataset_result.status_code != 200:
        get_dataset_result.raise_for_status()
        return

    return json.loads(get_dataset_result.text)


def get_current_dataset_type(dataset_result: dict) -> str:
    current_dataset = dataset_result.get("current", None)
    if current_dataset:
        return current_dataset.get("type", None)
    else:
        raise KeyError("'current' not found in dataset_result keys")


def check_dataset_type_is_static(dataset_api_client: DatasetAPIClient) -> bool:
    dataset_result = get_dataset_api_response(dataset_api_client)
    if not dataset_result:
        raise DatasetNotFoundException(
            "Dataset not found", dataset_path=dataset_api_client.dataset_path
        )

    dataset_type = get_current_dataset_type(dataset_result)
    if not dataset_type:
        raise DatasetTypeException("Dataset type error", dataset_type=str(dataset_type))

    return dataset_type == "static"
