import os
from abc import ABC, abstractmethod

from dpytools.slack.slack import SlackMessenger

from dpypelines.pipeline.shared.utils import (
    get_commit_id,
    get_environment,
    get_local_time,
    str_to_bool,
)


class BasePipelineNotifier(ABC):
    """
    Base pipeline notifier to ensure all variations of notifiers
    implement the required methods.
    """

    @abstractmethod
    def failure(self):
        """
        Notify that the pipeline has encountered an issue and failed.
        """
        ...

    @abstractmethod
    def success(self):
        """
        Notify that the pipeline has succeeded
        """
        ...

    # TODO - remove me later
    # only present while we're using the temporary "everything worked" message
    def msg_str(self, msg: str): ...


# see: https://www.techtarget.com/whatis/definition/no-op-no-operation
class NopNotifier(BasePipelineNotifier):
    """
    A no operation notifier so we can toggle off notifications locally,
    i.e methods exist but do nothing when called.
    """

    def failure(self): ...

    def success(self): ...


class PipelineNotifier(BasePipelineNotifier):
    def __init__(self, webhook_url, process_start_time=None):
        assert (
            webhook_url is not None
        ), "Unable to find required environment variable to populate webhook_url argument"
        self.client = SlackMessenger(webhook_url)
        self.notification_postfix = os.environ.get("NOTIFICATION_POSTFIX", "")
        self.process_start_time = process_start_time
        self.environment = get_environment()

    def failure(self, source_id=None):
        process_end_time = get_local_time()
        msg = f":x: {self.notification_postfix}, commit ID: {get_commit_id()}, source ID: {source_id}, processing start time: {self.process_start_time}, processing end time: {process_end_time}, environment: {self.environment}".strip()
        self.client.msg_str(msg)

    def success(self, source_id=None):
        process_end_time = get_local_time()
        msg = f":white_check_mark: {self.notification_postfix}, commit ID: {get_commit_id()}, source ID: {source_id}, processing start time: {self.process_start_time}, processing end time: {process_end_time}, environment: {self.environment}".strip()
        self.client.msg_str(msg)

    # TODO - remove me later
    # only present while we're using the temporary "everything worked" message
    def msg_str(self, msg: str):
        self.client.msg_str(msg)


def notifier_from_env_var_webhook(
    env_var: str, process_start_time=None
) -> BasePipelineNotifier:
    """
    Create a variant of BasePipelineMessenger by passing in the name
    of an envionrment variable that will hold the required webhook.
    """

    notifications_disabled = os.environ.get("DISABLE_NOTIFICATIONS", None)
    notifications_disabled = (
        False if notifications_disabled is None else str_to_bool(notifications_disabled)
    )

    if notifications_disabled is True:
        return NopNotifier()

    web_hook = os.environ.get(env_var, None)
    assert (
        web_hook is not None
    ), f"The specified env var {env_var} is not present on this system."

    return PipelineNotifier(web_hook, process_start_time)
