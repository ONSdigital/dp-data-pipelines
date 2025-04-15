import dataclasses
import json
from datetime import datetime
from typing import List

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.errors import (
    DatasetAPIRequestCreationException,
    DatasetNotFoundException,
    DatasetTypeException,
    DistributionsException,
)
from dpypelines.pipeline.messages.utils import get_mimetype
from dpypelines.pipeline.models import Distribution, Metadata

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
                dataclasses.asdict(distribution)
                for distribution in metadata.distributions
            ],
            "usage_notes": [
                dataclasses.asdict(usage_note) for usage_note in metadata.usage_notes
            ],
            "alerts": [dataclasses.asdict(alert) for alert in metadata.alerts],
            "release_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
        return dataset_path, edition_path, request_body
    except Exception as err:
        raise DatasetAPIRequestCreationException(
            "Error getting POST request values from metadata", metadata=metadata
        ) from err


# 2885 Delete this function and DistributionsException?
def get_distribution_details_for_request(distributions: List[Distribution]) -> list:
    """
    Get the information to populate the `distributions` property in the POST request to the Dataset API.
    """
    try:
        distributions = [
            {
                "title": distribution.title,
                "download_url": distribution.download_url,
                "byte_size": 0,
                "format": distribution.format,
                "media_type": distribution.media_type,
            }
            for distribution in distributions
        ]
        logger.info(
            "Distributions information retrieved",
            data={"distributions": distributions},
        )
        return distributions
    except Exception as err:
        raise DistributionsException(
            "Error getting details of distributions for Dataset API request",
            distributions=distributions,
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
    if dataset_result:
        dataset_type = get_current_dataset_type(dataset_result)
    else:
        raise DatasetNotFoundException(
            "Dataset not found", dataset_path=dataset_api_client.dataset_path
        )

    if dataset_type and dataset_type == "static":
        logger.info("Dataset type is static")
        return True
    elif dataset_type and dataset_type != "static":
        logger.info("Dataset type is not static")
        return False
    else:
        raise DatasetTypeException("Dataset type error", dataset_type=str(dataset_type))
