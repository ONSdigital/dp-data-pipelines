# Notifications

`dp-data-pipelines` has two different mechanisms to notify users and developers about events that occur during pipeline processing: **emails** and **Slack** messages. The code for the implementation of these notifications is contained in the directory [`dpypelines/pipeline/messages`](../dpypelines/pipeline/messages/), with some additional functionality coming from the [`dp-python-tools`](https://github.com/ONSdigital/dp-python-tools) repository, such as base client classes.

The sections below outline details of each notification method. Email and Slack clients can be instantiated in one step using the `setup_clients()` function in [setup.py](../dpypelines/pipeline/messages/setup.py), but these are two independent clients with differences in their implementation.

## Email notifications

Email functionality can be enabled or disabled by setting the `DISABLE_EMAILS` feature flag as appropriate. The first step in sending email notifications is the creation of an `SesClient` object, which requires a valid sender email address and AWS region to be instantiated. These values are retrieved from AWS Secrets Manager or environment variables (see the [config documentation](config.md) for more details).

The `SesClient` implements a `send()` method which validates the recipient email address and manages the email subject and content. The recipient email address is retrieved from the `manifest.json` file's `submission_contacts.email` field. Email templates are provided for a range of pipeline processing scenarios, such as validation or upload errors, and pipeline success and failure.

```python
from dpypelines.pipeline.config.job_config import get_job_config
from dpypelines.pipeline.messages.utils import get_email_client
from dpypelines.pipeline.messages.email_templates import submission_processed_email

config = get_job_config()
email_client = get_email_client(config=config)
submitter_email = "test@example.org"

# Send an email with the specified subject and body
email_client.send(
    recipient=submitter_email,
    subject="My email subject",
    body="This is the content of the email"
)

# Generate email template content for a successful submission
email_template = submission_processed_email()
email_client.send(
    recipient=submitter_email,
    subject=email_template.subject,
    body=email_template.message
)
```

## Slack notifications

Slack notifications can be enabled or disabled by setting the `DISABLE_NOTIFICATIONS` feature flag as appropriate. The first step in sending Slack notifications is the creation of an `PipelineNotifier` object, which requires a valid Slack channel webhook to be instantiated. This is retrieved from AWS Secrets Manager or environment variables (see the [config documentation](config.md) for more details).

The `PipelineNotifier` implements `success()` and `failure()` methods which manage the content of notifications sent to the specified Slack channel. These methods are configured to provide useful information such as processing start and end times, the commit hash of the version of `dp-data-pipelines` that was used to process the submission, and the current `environment`.

```python
from dpypelines.pipeline.config.job_config import get_job_config
from dpypelines.pipeline.messages.setup import get_notifier

config = get_job_config()
notifier = get_notifier(webhook=config.de_slack_webhook)

# Send a success notification
notifier.success()

# Send a failure notification
notifier.failure()
```