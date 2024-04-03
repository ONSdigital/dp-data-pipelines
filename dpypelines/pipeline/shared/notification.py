import os

from dpytools.slack.slack import SlackMessenger


isDisabled = os.environ.get("DISABLE_NOTIFICATIONS", 'False').lower()

class PipelineMessenger(SlackMessenger):
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
        #check for the variable, if its not set or set to flase act as previously
        if not isDisabled:
            assert (
                self.webhook_url is not None
            ), "Unable to find required environment variable to populate webhook_url argument"
            super().__init__(self.webhook_url)
        else:
            super().__init__(self)

    def failure(self):
        self.msg_str(":boom:")

    def success(self):
        self.msg_str(":white_check_mark:")
