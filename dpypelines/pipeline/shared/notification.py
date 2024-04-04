from __future__ import annotations

from abc import ABC, abstractmethod
import os

from dpytools.slack.slack import SlackMessenger
from dpypelines.pipeline.shared.utility import str_to_bool


class BasePipelineNotifier(ABC):
    """
    Base pipeline messenger to ensure all variations of this
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
    def msg_str(self, msg: str):
        ...

# see: https://www.techtarget.com/whatis/definition/no-op-no-operation
class NopNotifier(BasePipelineNotifier):
    """
    A no operation messenger so we can toggle off notifications locally,
    i.e methods exist but do nothing when called.
    """

    def failure(self):
        ...

    def success(self):
        ...

    # TODO - remove me later
    # only present while we're using the temporary "everything worked" message
    def msg_str(self, _):
        ...


class PipelineNotifier(BasePipelineNotifier, SlackMessenger):
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        assert (
            self.webhook_url is not None
        ), "Unable to find required environment variable to populate webhook_url argument"
        super().__init__(self.webhook_url)

    def failure(self):
        self.msg_str(":boom:")

    def success(self):
        self.msg_str(":white_check_mark:")


def notifier_from_env_var_webhook(env_var: str) -> BasePipelineNotifier:
    """
    Create a variant of BAsePipelineMessenger by passing in the name
    of an envionrment variable that will hold the required webhook.
    """

    notifications_disabled = os.environ.get("DISABLE_NOTIFICATIONS", False)
    if notifications_disabled is not False:
        notifications_disabled = str_to_bool(notifications_disabled)

    if notifications_disabled:
        return NopNotifier()

    web_hook = os.environ.get(env_var, None)
    assert web_hook is not None, f"The specified env var {env_var} is not present on this system."

    return PipelineNotifier(web_hook)

