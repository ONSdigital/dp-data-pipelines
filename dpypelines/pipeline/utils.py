from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.shared.notification import (
    PipelineNotifier,
    notifier_from_env_var_webhook,
)
from dpypelines.pipeline.shared.utils import get_local_time

logger = DpLogger("data-ingress-pipelines")


def get_source_id(manifest_dict: dict) -> str:
    """
    This function returns the `source_id` form the provided manifest_dict (which is the data in the manifest.json file).
    """
    return manifest_dict["source_id"]


def get_notifier():
    # Create notifier from webhook env var
    try:
        process_start_time = get_local_time()
        notifier: PipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK",
            process_start_time=process_start_time,
        )
        logger.info("Notifier created", data={"notifier": notifier})
        return notifier
    except Exception as err:
        logger.error("Error occurred when creating notifier", err)
        raise err


def get_post_request_values_from_metadata(metadata: dict):
    """
    Generate the required path values and request body to submit to the Dataset API. This will be submitted as a POST request to the endpoint `/datasets/{dataset_path}/editions/{edition_path}/versions`
    """
    try:
        dataset_path = metadata["dcterms:identifier"]
        edition = metadata["TBC:edition"][0]
        edition_path = edition["dcterms:identifier"]
        distributions = get_download_details_for_request(edition["dcat:distribution"])

        request_body = {
            # Required properties (from swagger.yaml)
            # Tier 0 metadata standards - required with output
            "title": metadata["dcterms:title"],
            "description": metadata["dcterms:description"],
            "next_release": metadata["TBC:nextRelease"],
            # Tier 0 metadata standards - added during publishing
            "type": "static",
            "state": "associated",
            "release_date": "The release date of this version of the dataset",
            "themes": metadata["dcat:theme"],
            # TODO `links:spatial` and `links:job` fields to be removed from data model - hardcode for now to allow request to succeed
            "links": {
                "spatial": {"href": "string"},
                "job": {"href": "string", "id": "string"},
            },
            # Additional properties
            "alerts": edition["TBC:alerts"],
            # TODO Calculate `downloads` size during processing
            "downloads": distributions,
            "usage_notes": edition["TBC:usage_notes"],
            # Not included here ($ref: '#/definitions/Version')
            # collection_id (auto generated?)
            # dimensions $ref: '#/definitions/Dimension'
            # edition (readOnly - auto generated?)
            # id (auto generated?)
            # is_based_on (census only)
            # latest_changes $ref: '#/definitions/LatestChange'
            # links (auto generated?)
            # lowest_geography (census only)
            # temporal $ref: '#/definitions/Temporal'
            # version (readOnly - auto generated?)
        }
        return dataset_path, edition_path, request_body
    except Exception as err:
        logger.error(
            "Error getting POST request values from metadata",
            err,
            data={"metadata": metadata},
        )
        raise err


def get_download_details_for_request(distributions: list) -> dict:
    try:
        downloads = {
            distribution["TBC:distributionFormat"]: {
                "href": "The URL to the generated file",
                "size": "The size of the file in bytes",
            }
            for distribution in distributions
        }
        return downloads
    except Exception as err:
        logger.error(
            "Error getting details of download files for Dataset API request",
            err,
            data={"distributions": distributions},
        )
