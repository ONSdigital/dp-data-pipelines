import boto3
import json
from botocore.exceptions import ClientError


def get_secret(value: str) -> str:

    secret_name = "dp-sandbox-secrets"
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    data = json.loads(secret)
    return data[value]


print(get_secret("DE_SLACK_WEBHOOK"))