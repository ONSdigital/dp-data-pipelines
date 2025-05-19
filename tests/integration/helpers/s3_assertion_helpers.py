from typing import Optional
from moto import mock_aws
import pytest
from botocore.exceptions import ClientError


def get_objects_in_bucket(s3_client, bucket_name: str):
    files_in_dir = s3_client.list_objects(Bucket=bucket_name)
    contents = files_in_dir["Contents"]
    return contents


def get_object_keys_in_bucket(s3_client, bucket_name: str):
    contents = get_objects_in_bucket(s3_client, bucket_name)
    return [content["Key"] for content in contents]


def assert_files_exist(
    object_keys_in_bucket: list[str], expected_files: list[str], expected_prefix: str
):
    for file in expected_files:
        matching = get_matching_file(object_keys_in_bucket, file, expected_prefix)
        assert matching is True, f"Did not find {file} in {object_keys_in_bucket}"


def get_matching_file(object_keys: list[str], file_name: str, expected_prefix: str):
    for key in object_keys:
        if key.endswith(file_name) and key.startswith(expected_prefix):
            return True

    return False


class S3ObjectFile:
    def __init__(self, uploaded_path: str):
        self.uploaded_path = uploaded_path
        split_key = uploaded_path.split("/")
        self.bucket_name = split_key[0]
        self.initial_key_without_bucket = uploaded_path.removeprefix(
            self.bucket_name
        ).removeprefix("/")
        self.file_name = split_key[-1]

    @mock_aws
    def verify_file_moved(self, s3_client):
        with pytest.raises(ClientError) as e:
            s3_client.head_object(
                Bucket=self.bucket_name, Key=self.initial_key_without_bucket
            )

        assert "(404)" in str(e)

    @mock_aws
    def verify_s3_object_in_directory(
        self,
        s3_client,
        original_s3_object_key: str,
        key_prefix: str,
        expected_file_count: Optional[int] = None,
    ):
        file_name = original_s3_object_key.split("/").pop()
        self.verify_files_in_destination(
            s3_client, [file_name], key_prefix, expected_file_count
        )

    @mock_aws
    def verify_files_in_destination(
        self,
        s3_client,
        expected_files: list[str],
        key_prefix: str,
        expected_file_count: Optional[int] = None,
    ):
        object_keys_in_directory = get_object_keys_in_bucket(
            s3_client, self.bucket_name
        )
        if expected_file_count is not None:
            assert len(object_keys_in_directory) == expected_file_count, (
                object_keys_in_directory
            )
        else:
            assert len(object_keys_in_directory) == len(expected_files), (
                object_keys_in_directory
            )

        assert_files_exist(object_keys_in_directory, expected_files, key_prefix)
