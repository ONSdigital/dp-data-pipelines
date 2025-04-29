from unittest.mock import MagicMock
from dpypelines.pipeline.messages.notification import PipelineNotifier


class MockPipelineNotifier(PipelineNotifier):
    def __init__(self, webhook_url, process_start_time=None):
        super().__init__(webhook_url=webhook_url, process_start_time=process_start_time)
        slack_mock = MagicMock()
        slack_mock.msg_str.return_value = None
        self.client = slack_mock
