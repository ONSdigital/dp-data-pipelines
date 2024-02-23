import logging

from base import BaseHttpClient


class SlackNotifier:
    def __init__(self, webhook_url):
        if not webhook_url:
            raise ValueError("webhook_url is not set")
        self.webhook_url = webhook_url
        self.http_client = BaseHttpClient()

    def notify(self, msg_dict: dict):
        """
        Send a message to the Slack webhook.

        The msg_dict parameter should be a dictionary that matches the
        structure documented at https://api.slack.com/messaging/webhooks
        """
        try:
            response = self.http_client.post(self.webhook_url, json=msg_dict)
            response.raise_for_status()
        except Exception as e:
            logging.error(f"Failed to send notification: {e}")

    def msg_str(self, msg: str):
        """
        Send a string message to the Slack webhook.

        The msg parameter is wrapped into a dictionary before being sent.
        """
        self.notify({"text": msg})