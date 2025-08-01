# Error handling

Error handling in `dp-data-pipelines` is implemented in [`error_handler.py`](../dpypelines/pipeline/messages/error_handler_module.py).

## `error_handler`

The `error_handler` function handles all pipeline errors, allowing errors to be raised while simultaneously managing logs, emails and Slack notifications. Function arguments are listed in the table below:

### Error details

| Argument name         | Description                                                                      | Required/Optional (Default) | Data type            |
|:----------------------|:---------------------------------------------------------------------------------|:----------------------------|:---------------------|
| `error`               | The error to be raised.                                                          | Required                    | Exception            |
| `data`                | Additional data to include in the logs.                                          | Optional                    | Dict[str, Any]       |
| `submitter_email`     | The email address for sending email notifications.                               | Required                    | str                  |
| `enable_logs`         | For enabling/disabling logging in code.                                          | Required (True)             | boolean              |
| `enable_email`        | For enabling/disabling the sending of email notifications when the error occurs. | Required (True)             | boolean              |
| `enable_notification` | For enabling/disabling Slack notifications when the error occurs.                | Required (True)             | boolean              |
| `notifier`            | The notifier client to use for sending Slack notifications about the error.      | Optional (None)             | EmailClient          |
| `email_client`        | The email client to use for sending email notifications about the error.         | Optional (None)             | BasePipelineNotifier |
