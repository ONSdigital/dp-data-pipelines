import json
from datetime import datetime

from dpytools.http.api.dataset_api_client import DatasetAPIClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.errors import (
    DatasetAPIRequestCreationException,
    DatasetNotFoundException,
    DatasetTypeException,
    DistributionsException,
)

logger = DpLogger("data-ingress-pipelines")


def get_post_request_values_from_metadata(metadata: dict):
    """
    Generate the required path values and request body to submit to the Dataset API. This will be submitted as a POST request to the endpoint `/datasets/{dataset_path}/editions/{edition_path}/versions`
    """
    try:
        dataset_path = metadata.get("dcterms:identifier", None)
        editions = metadata.get("TBC:edition", None)
        if editions is not None:
            edition: dict = editions[0]
        else:
            edition = {"dcterms:identifier": None}
        edition_path = edition.get("dcterms:identifier", None)
        metadata_distributions = edition.get("dcat:distribution", None)
        if metadata_distributions is not None:
            distributions = get_distribution_details_for_request(metadata_distributions)
        else:
            distributions = None

        request_body = {
            "distributions": distributions,
            "release_date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "edition_title": edition.get("dcterms:title", None),
            "title": metadata.get("dcterms:title", None),
            "description": metadata.get("dcterms:description", None),
            "next_release": metadata.get("TBC:nextRelease", None),
            "themes": metadata.get("dcat:theme", None),
            "alerts": edition.get("TBC:alerts", None),
            "usage_notes": edition.get("TBC:usage_notes", None),
            "quality_designation": edition.get("TBC:quality_designation", None),
        }
        return dataset_path, edition_path, request_body
    except Exception as err:
        raise DatasetAPIRequestCreationException(
            "Error getting POST request values from metadata", metadata=metadata
        ) from err


def get_distribution_details_for_request(metadata_distributions: list) -> list:
    """
    Get the information to populate the `distributions` property in the POST request to the Dataset API.
    """
    try:
        distributions = [
            {
                "title": distribution["dcterms:title"],
                "download_url": distribution["download_url"],
                # TODO Calculate byte_size during processing
                "byte_size": 0,
                "format": distribution["TBC:distributionFormat"],
                "media_type": distribution["dcat:mediaType"],
            }
            for distribution in metadata_distributions
        ]
        logger.info(
            "Distributions information retrieved",
            data={"distributions": distributions},
        )
        return distributions
    except Exception as err:
        raise DistributionsException(
            "Error getting details of distributions for Dataset API request",
            distributions=metadata_distributions,
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
