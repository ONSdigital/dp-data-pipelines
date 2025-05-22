"""
Lambda function code that is triggered by the appearance of an object in
an s3 source bucket.

Note: upon recieving a notification the entry point for this code is the
function lambda_handler.
"""

import json
import logging
import os
import urllib
from urllib.error import HTTPError

import boto3

logger = logging.getLogger()

# There are two important constants here defining values expected elsewhere in the codebase:
# 1. "S3_OBJECT_KEY": defines the key for a key value pair, the ETL Lambda will look for this key.
S3_OBJECT_KEY = "s3-object-name"
ENVIRONMENT = os.environ.get("ENVIRONMENT")


def get_env_variable(key: str) -> str:
    value = os.environ.get(key, None)
    if value is None:
        raise ValueError(f"Mandatory environment variable {key} not detected.")
    return value


# Fail early and raise if we don't have a webhook for lambda failure notifications.
NOTIFICATION_WEBHOOK = get_env_variable("NOTIFICATION_WEBHOOK")
OTHER_LAMBDA_ARN = get_env_variable("OTHER_LAMBDA_ARN")
client = boto3.client("lambda")


def create_log_stream_url(context) -> str:
    """
    Creates a url linking back to the log stream for this lambda invocation.
    """
    # Retrieve AWS region and AWS Lambda function name
    region_name = os.environ["AWS_REGION"]
    function_name = context.function_name

    # Initialize CloudWatch Logs client
    logs_client = boto3.client("logs", region_name=region_name)

    # Retrieve the log group name for the Lambda function
    log_group_name = "/aws/lambda/" + function_name

    # Describe log streams to get the latest log stream
    response = logs_client.describe_log_streams(
        logGroupName=log_group_name, orderBy="LastEventTime", descending=True, limit=1
    )

    # Extract the log stream name
    log_stream_name = response["logStreams"][0]["logStreamName"]

    # Construct the CloudWatch log stream URL
    log_stream_url = f"https://console.aws.amazon.com/cloudwatch/home?region={region_name}#logEventViewer:group={log_group_name};stream={log_stream_name}"

    return log_stream_url


def handle_error(context, initial_err: Exception):
    """
    Function that sends a notification to slack in the event that
    this lambda encounters an error, then raises said error.
    """
    # We're expecting to be told the environment name by an env var that
    # should always be there, but log without raising as a precaution.
    # Better a notification missing something than no notification.
    try:
        environment = os.environ["ENVIRONMENT"]
    except Exception as err:
        logger.error(
            f"Unable to acquire name of envionrment from ENVIRONMENT env var: {err}"
        )
        environment = "not-specified (see logs)"

    # Now create the text for the slack message
    message_text = f""":x: `{environment}` - ingest lambda encountered an error:
        ```
        {str(initial_err).strip()}
        ```
        """

    # Try and get a url for the log stream of this lambda.
    # Log but dont raise any errors. Better to have a notification
    # without a log stream url than no notification at all.
    log_stream_url = None
    try:
        log_stream_url = create_log_stream_url(context)
    except Exception as err:
        logger.error(f"Unable to create log stream url: {err}")

    # Append the log stream url to the message where we've got one.
    if log_stream_url is not None:
        message_text += log_stream_url

    # Structure and encode message payload as slack expects
    message = {"text": message_text}
    encoded_data = json.dumps(message).encode("utf-8")

    try:
        # Send POST request to Slack webhook
        req = urllib.request.Request(
            NOTIFICATION_WEBHOOK,
            data=encoded_data,
            headers={"Content-Type": "application/json"},
        )
        req.method = "POST"
        response = urllib.request.urlopen(req)

        # Raise if the POST does not work for whatever reason
        if response.status != 200:
            raise HTTPError(
                NOTIFICATION_WEBHOOK,
                response.status_code,
                f"Slack POST returned status code {response.status_code} from {NOTIFICATION_WEBHOOK}",
                [],
                None,
            )
        logger.info(f"Notification posted to slack, status code: {response.status}")

    # If sending a notification error did not work, we need to be careful
    # to include both this new notification_error as well as whatever pipeline
    # initial_err triggered this function.
    except Exception as notification_err:
        raise Exception(
            f"""
            Error encountered when attempting to send notifcation of error.

            Notification error was: {notification_err}

            Original error follows in stack trace."""
        ) from initial_err

    # Notification to slack worked. Just raise the error to stop processing.
    raise initial_err


def lambda_handler(event, context):
    """
    Lambda function that is triggered by an aws s3:PutObject event, where the bucket in question is the data sources submission bucket.

    The purpose of this lambda is to look at the file extension and invoke the ETL Lambda Function
    """
    results = []
    for record in event["Records"]:
        s3_object_name = get_s3_object_name(context, record)
        response = process_s3_object(context, s3_object_name)
        results.append(response)
    return {
        "statusCode": 200,
        "body": json.dumps({"results": results}),
    }


def process_s3_object(context, s3_object_name: str):
    if s3_object_name.endswith(".zip"):
        response = trigger_other_lambda(context, s3_object_name)
        return response

    msg = f"""
            Received notification of invalid file submission: {s3_object_name}
            Submitted files are expected to have the ".zip" file extension.
            """
    logger.error(msg)
    handle_error(context, ValueError(msg))


def get_s3_object_name(context, record) -> str:
    try:
        object_key = urllib.parse.unquote_plus(
            record["s3"]["object"]["key"], encoding="utf-8"
        )
        bucket_name = record["s3"]["bucket"]["name"]

        # Determine the folder path by taking everything before the last slash.
        if "/" not in object_key:
            raise ValueError(
                f"Object key {object_key} does not appear to be in a folder structure."
            )

        object_name = f"{bucket_name}/{object_key}"
        logger.info(f"S3 object name {object_name}")
        return object_name
    except KeyError as err:
        logger.error(
            f"KeyError when attempting to get bucket name and object key: {str(err)}"
        )
        handle_error(context, err)


def trigger_other_lambda(context, s3_object_name: str):
    try:
        payload = json.dumps({"S3_OBJECT_KEY": s3_object_name})
        logger.info(f"Triggering Lambda {OTHER_LAMBDA_ARN} with payload {payload}")
        response = client.invoke(
            FunctionName=OTHER_LAMBDA_ARN,
            InvocationType="RequestResponse",
            Payload=payload,
        )

        responseJson = json.loads(response["Payload"])

        logger.info(f"Started job run of id: {responseJson}")

        return responseJson
    except Exception as err:
        logger.error(
            f"Error encountered when trying to start Lambda function {OTHER_LAMBDA_ARN}: {str(err)}"
        )
        handle_error(context, err)
