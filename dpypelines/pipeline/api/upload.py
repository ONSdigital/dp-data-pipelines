from datetime import datetime
from pathlib import Path
from typing import List
from uuid import uuid4
from dpypelines.pipeline.config.job_config import JobConfig
from dpytools.logging.logger import DpLogger
from dpypelines.pipeline.messages.utils import (
    get_mimetype,
)
from dpytools.http.upload.upload_service_client import UploadServiceClient

logger = DpLogger("data-ingress-pipeline")


def create_upload_service_client(config: JobConfig):
    return UploadServiceClient(config.upload_service_url)


def upload_files(
    files_to_upload: List[Path], config: JobConfig, upload_client: UploadServiceClient
):
    """Upload files and send notifications."""
    for required_file_path in files_to_upload:
        logger.info(
            "Uploading file to Upload Service API",
            data={
                "file_path": required_file_path,
                "upload_url": config.upload_service_url,
            },
        )
        mimetype = get_mimetype(Path(required_file_path).suffix)
        if not mimetype:
            err_msg = f"Uploading file type {Path(required_file_path).suffix} not supported for file: {required_file_path}."
            raise NotImplementedError(err_msg)

        timestamp = datetime.now()
        identifier = f"{timestamp.strftime('%d-%m-%yT%H-%M-%S')}-{uuid4()}-{required_file_path.name.replace('.', '-')}"
        # identifier = f"{timestamp}-{required_file_path.name.replace('.', '-')}"
        upload_path = f"datasets/{identifier}"

        upload_service_response = upload_client.upload_new(
            file_path=required_file_path,
            mimetype=mimetype,
            upload_path=upload_path,
            identifier=identifier,
        )
        upload_service_response.raise_for_status()

        logger.info(
            "File uploaded",
            data={
                "file_path": required_file_path,
                "upload_url": config.upload_service_url,
            },
        )
