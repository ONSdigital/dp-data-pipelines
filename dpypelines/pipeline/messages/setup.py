from dpypelines.pipeline.config.job_config import JobConfig
from dpypelines.pipeline.messages.utils import get_email_client
from dpytools.logging.logger import DpLogger
from dpypelines.pipeline.messages.notification import (
    BasePipelineNotifier,
    NopNotifier,
    PipelineNotifier,
)
from dpypelines.pipeline.messages.utils import get_local_time

logger = DpLogger("data-ingress-pipeline")


def create_notifier(
    webhook: str, process_start_time=None, disable_notifications: bool = False
) -> BasePipelineNotifier:
    """
    Create a variant of BasePipelineMessenger by passing in a webhook.
    Enables use of webhooks from the AWS secrets manager rather than env vars.
    """
    if disable_notifications is True:
        return NopNotifier()

    return PipelineNotifier(webhook, process_start_time)


def get_notifier(config: JobConfig):
    # Create notifier from webhook env var
    try:
        process_start_time = get_local_time()
        notifier: BasePipelineNotifier = create_notifier(
            config.de_slack_webhook,
            process_start_time=process_start_time,
            disable_notifications=config.disable_notifications,
        )
        logger.info("Notifier created", data={"notifier": notifier})
        return notifier
    except Exception as err:
        logger.error("Error occurred when creating notifier", error=err)
        raise err


def setup_clients(config: JobConfig):
    """Set up clients for notification and email."""
    notifier = get_notifier(config)
    email_client = get_email_client(config)

    logger.info(
        "Clients set up successfully",
        data={"notifier": notifier, "email_client": email_client},
    )
    return notifier, email_client
