import os

from dpytools.slack.slack import SlackMessenger


class PipelineMessenger(SlackMessenger):
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
