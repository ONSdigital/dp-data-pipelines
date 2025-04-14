import io
import os

import boto3

ENVIRONMENT = os.environ.get("ENVIRONMENT", "sandbox")
bucket_name = f"dp-{ENVIRONMENT}-ingest-submission-bucket"
job_name = f"dp-{ENVIRONMENT}-ingest-from-s3-object-created"
s3_object_path: str = "input/"

SUCCESS_STATUS = "SUCCEEDED"
ERRORED_STATUS = "ERRORED"
RUNNING_STATUS = "RUNNING"

TIMEOUT_FOR_RUNNING_STATUS = 2
TIMEOUT_FOR_FINISH_STATUS = 50


def upload_and_test(file: io.BytesIO, file_name: str):
    s3_client = boto3.client("s3", endpoint_url=os.getenv("AWS_ENDPOINT_URL"))
    object_key = f"{s3_object_path}{file_name}"

    print(f"Uploading file {object_key}")

    result = s3_client.put_object(
        Bucket=bucket_name, Key=object_key, Body=file.getvalue()
    )

    return process_response(result, object_key)


def process_response(result: dict, object_key: str):
    if result is None:
        raise Exception(f"Did not receive response uploading {object_key}")
    response_metadata = result.get("ResponseMetadata", None)

    if response_metadata is None:
        raise Exception(f"No response metadata in response for {object_key}")

    status = response_metadata.get("HTTPStatusCode", None)

    if status is None:
        raise Exception(f"Could not find HTTPStatusCode for {object_key}")

    if status < 200 or status > 299:
        raise Exception(
            f"Did not receive success response for {object_key}, received {status}"
        )

    print(f"Successfully uploaded {object_key}")
    return result
