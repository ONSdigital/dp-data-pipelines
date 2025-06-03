"""
Lambda function to attempt to retry executing failed ETL processes,
giving configurable retry logic such as
maximum retry attempts (default: 5) or backoff timing.
"""

import json
import logging
import os

import boto3

from dpypelines.pipeline.shared.shared_lambda import lambda_utils

logger = logging.getLogger()
logger.setLevel("INFO")

MAXIMUM_RETRY_ATTEMPTS = 5
BACKOFF_TIMING_SCHEDULE = None

OTHER_LAMBDA_ARN = lambda_utils.get_env_variable("OTHER_LAMBDA_ARN")

client = boto3.client("lambda")


def lambda_handler(event: dict, context):
    """
    Lambda function to detect when an ETL process has failed,
    then attempt to retry the process while iterating on
    retry variables (retry attempts and exponential backoff).
    """
    try:
        logger.info("Retry lambda checking ETL process status.")

        event_status = get_event_status_type(event)
        if event_status == "FAILED":
            for record in event["Records"]:
                # invoke etl lambda again
                logger.info(f"ETL process failure detected, retrying")

                s3_object_name = lambda_utils.get_s3_object_name(context, record)

                lambda_utils.trigger_other_lambda(
                    context, s3_object_name, client, OTHER_LAMBDA_ARN
                )

        else:
            # ETL process did not fail, do nothing
            logger.info("ETL process status normal, continuing")
            pass
    except Exception as e:
        logger.error(f"Error occured while attempting to retry ETL process. Error: {e}")
        raise e


def get_event_status_type(event):
    try:
        if isinstance(event, str):
            event = json.loads(event)

        if "event_type" in event:
            return event["event_type"]

    except Exception as e:
        logger.error(f"Error occured while retrieving ETL process status: {e}")
        raise e
