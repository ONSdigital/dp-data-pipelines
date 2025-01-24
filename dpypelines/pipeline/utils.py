from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.http.api.dataset_api_client import DatasetAPIClient
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


def get_upload_client(upload_url):
    # Upload output files to Upload Service
    try:
        # Create UploadClient from upload_url
        client = UploadServiceClient(upload_url)
        logger.info(
            "UploadClient created from upload_url", data={"upload_url": upload_url}
        )
        return client
    except Exception as err:
        logger.error(
            "Error creating UploadClient", err, data={"upload_url": upload_url}
        )
        raise err


def get_dataset_api_client(dataset_api_url, dataset_id):
    try:
        # Create DatasetAPIClient from dataset_api_url and dataset_id
        client = DatasetAPIClient(dataset_api_url, dataset_id)
        logger.info(
            "DatasetAPIClient created for dataset_id provided",
            data={"dataset_api_url": dataset_api_url, "dataset_id": dataset_id},
        )
        return client
    except Exception as err:
        logger.error(
            "Error creating DatasetAPIClient",
            err,
            data={"dataset_api_url": dataset_api_url, "dataset_id": dataset_id},
        )
        raise err


def get_secondary_function(config_dict: dict):
    return config_dict["secondary_function"]


def get_dataset_id_from_metadata(metadata: dict) -> str:
    return metadata["dcterms:identifier"]
