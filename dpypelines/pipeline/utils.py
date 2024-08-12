from dpytools.http.upload import UploadServiceClient
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.shared.notification import (
    BasePipelineNotifier,
    notifier_from_env_var_webhook,
)

logger = DpLogger("data-ingress-pipelines")


def notifier():
    # Create notifier from webhook env var
    try:
        de_notifier: BasePipelineNotifier = notifier_from_env_var_webhook(
            "DE_SLACK_WEBHOOK"
        )
        logger.info("Notifier created", data={"notifier": de_notifier})
    except Exception as err:
        logger.error("Error occurred when creating notifier", err)
        raise err


def upload_file(upload_url):
    # Upload output files to Upload Service
    try:
        # Create UploadClient from upload_url
        UploadServiceClient(upload_url)
        logger.info(
            "UploadClient created from upload_url", data={"upload_url": upload_url}
        )
    except Exception as err:
        logger.error(
            "Error creating UploadClient", err, data={"upload_url": upload_url}
        )
        notifier.failure()
        raise err
