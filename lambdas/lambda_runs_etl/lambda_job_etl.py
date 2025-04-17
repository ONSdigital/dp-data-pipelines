"""
Glue script that is called to handle the appearance of a tar file in
the s3 source bucket.

This script should only ever parse arguments, set env vars and call
the dp-data-pipelines repo.

Application logic should not live in this infrastructure repo.
"""

import json
import logging
import os

logger = logging.getLogger()
logger.setLevel("INFO")

S3_OBJECT_KEY = "S3_OBJECT_KEY"
NOTIFICATION_POSTFIX = "NOTIFICATION_POSTFIX"

def _set_env_label_and_glue_job_url():
    """
    Helper to Add the name of the environment and the url to this Lamba function
    as an env var of NOTIFICATION_POSTFIX.
    """
    try:
        lambda_name = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "No name found")
        environment = os.environ["ENVIRONMENT"]

        os.environ[NOTIFICATION_POSTFIX] = (
            f"`{environment}` - {lambda_name}"
        )
    except Exception as e:
        logger.error(f'Error setting notification prefix variable "{NOTIFICATION_POSTFIX}": {e}', exc_info=True)
        raise e

def lambda_handler(event: dict, context):
    """
    Lambda function that is triggered by the other Lambda, after the first Lambda receives the S3 PutObject event. 

    This Lambda processes the S3 object.
    - Validate the file received
    - Unzip
    - Upload to relevant services
    """
    try:
        logger.info(f"Lambda ETL job received args: {event}")

        s3_object_key = get_s3_object_key(event)

        logger.info(f"Retrieved s3 object key: {s3_object_key}")

        _set_env_label_and_glue_job_url()

        from dpypelines import s3_folder_received

        # Call the pipeline code for processing zip file
        s3_folder_received.start(s3_object_key)
    except Exception as e:
        logger.error(f'Error processing received S3 object. Error: "{e}"', exc_info=True)
        raise e

def get_s3_object_key(event: dict) -> str:
    try:
        if isinstance(event, str):
            event = json.loads(event)

        if S3_OBJECT_KEY in event:
            return event[S3_OBJECT_KEY]

        raise KeyError(f'Could not find S3 object key "{S3_OBJECT_KEY}" in Lambda event. Event received: "{event}"')
    except Exception as e:
        logger.error(f'Error getting S3 object key: "{e}"', exc_info=True)
        raise e
