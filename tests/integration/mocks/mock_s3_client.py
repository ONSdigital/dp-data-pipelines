from typing import Optional
from unittest.mock import MagicMock
import boto3


def create_mock_s3_client(
    mock_s3_get: Optional[MagicMock] = None,
    mock_s3_put: Optional[MagicMock] = None,
    mock_s3_copy: Optional[MagicMock] = None,
    mock_s3_delete: Optional[MagicMock] = None,
):
    actual_client = boto3.client("s3")

    mock = MagicMock()

    mock.get_object.side_effect = (
        mock_s3_get if mock_s3_get is not None else actual_client.get_object
    )
    mock.put_object.side_effect = (
        mock_s3_put if mock_s3_put is not None else actual_client.put_object
    )
    mock.download_fileobj.side_effect = (
        mock_s3_get if mock_s3_get is not None else actual_client.download_fileobj
    )
    mock.copy_object.side_effect = (
        mock_s3_copy if mock_s3_copy is not None else actual_client.copy_object
    )
    mock.delete_object.side_effect = (
        mock_s3_delete if mock_s3_delete is not None else actual_client.delete_object
    )
    mock.actual_client = actual_client
    return mock
