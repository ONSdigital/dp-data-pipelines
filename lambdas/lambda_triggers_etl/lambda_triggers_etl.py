"""
Lambda function code that is triggered by the appearance of an object in
an s3 source bucket.

Note: upon recieving a notification the entry point for this code is the
function lambda_handler.
"""

import json
import logging
import os
import boto3

from dpypelines.pipeline.shared.shared_lambda.lambda_utils import (
    get_env_variable,
    get_s3_object_name,
    handle_error,
    trigger_other_lambda,
)

logger = logging.getLogger()

# There are two important constants here defining values expected elsewhere in the codebase:
# 1. "S3_OBJECT_KEY": defines the key for a key value pair, the ETL Lambda will look for this key.
S3_OBJECT_KEY = "s3-object-name"
ENVIRONMENT = os.environ.get("ENVIRONMENT")

OTHER_LAMBDA_ARN = get_env_variable("OTHER_LAMBDA_ARN")

client = boto3.client("lambda")


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
        response = trigger_other_lambda(
            context, s3_object_name, client, OTHER_LAMBDA_ARN
        )
        return response

    msg = f"""
            Received notification of invalid file submission: {s3_object_name}
            Submitted files are expected to have the ".zip" file extension.
            """
    logger.error(msg)
    handle_error(context, ValueError(msg))
