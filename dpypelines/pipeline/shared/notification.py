import os

from dpytools.slack.slack import SlackMessenger
from dpypelines.pipeline.shared.utility import enrich_online

class PSMessenger(SlackMessenger):
    def __init__(self):
        self.webhook_url = os.environ.get("PS_SLACK_WEBHOOK", None)
        assert (
            self.webhook_url is not None
        ), "Unable to find required environment variable PS_SLACK_WEBHOOK"
        super().__init__(self.webhook_url)

    def failure(self):
        self.msg_str("Failure")

    def success(self):
        self.msg_str("Success")


class DEMessenger(SlackMessenger):
    def __init__(self):
        self.webhook_url = os.environ.get("DE_SLACK_WEBHOOK", None)
        assert (
            self.webhook_url is not None
        ), "Unable to find required environment variable DE_SLACK_WEBHOOK"
        super().__init__(self.webhook_url)

    def failure(self):
        enrich_message = os.environ.get('ENRICH_OUTGOING_MESSAGES', None)
        if enrich_message is not None:
            self.msg_str(f":boom: {enrich_message}")
        self.msg_str(":boom:")

    def success(self):
        self.msg_str("Success")


class DSMessenger(SlackMessenger):
    def __init__(self):
        self.webhook_url = os.environ.get("DS_SLACK_WEBHOOK", None)
        assert (
            self.webhook_url is not None
        ), "Unable to find required environment variable DS_SLACK_WEBHOOK"
        super().__init__(self.webhook_url)

    def failure(self):
        self.msg_str("Failure")

    def success(self):
        self.msg_str("Success")


class SEMessenger(SlackMessenger):
    def __init__(self):
        self.webhook_url = os.environ.get("SE_SLACK_WEBHOOK", None)
        assert (
            self.webhook_url is not None
        ), "Unable to find required environment variable SE_SLACK_WEBHOOK"
        super().__init__(self.webhook_url)

    def failure(self):
        self.msg_str("Failure")

    def success(self):
        self.msg_str("Success")
