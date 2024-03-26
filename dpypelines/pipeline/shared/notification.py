import os

from dpytools.slack.slack import SlackMessenger


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
        self.msg_str("Failure")

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
