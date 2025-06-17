from pathlib import Path
from typing import List
from dpypelines.pipeline.config.job_config import JobConfig
from dpytools.logging.logger import DpLogger
from dpytools.http.upload.upload_service_client import UploadServiceClient

from dpypelines.pipeline.models.metadata_models import Distribution

logger = DpLogger("data-ingress-pipeline")


def create_upload_service_client(config: JobConfig):
    return UploadServiceClient(config.upload_service_url)


def upload_files(
    decompressed_file_dir: Path,
    distributions: List[Distribution],
    config: JobConfig,
    upload_client: UploadServiceClient,
):
    """Upload files and send notifications."""
    for distribution in distributions:
        file_path = decompressed_file_dir / distribution.file

        logger.info(
            "Uploading file to Upload Service API",
            data={
                "file_path": file_path,
                "upload_url": config.upload_service_url,
                "distribution": distribution.model_dump(),
            },
        )

        if not distribution.media_type:
            err_msg = f"Uploading file type {Path(file_path).suffix} not supported for file: {distribution}."
            raise NotImplementedError(err_msg)

        upload_client.upload_new(
            file_path=file_path,
            mimetype=distribution.media_type,
            upload_path=distribution.upload_path,
            identifier=distribution.identifier,
        )

        logger.info(
            "File uploaded",
            data={
                "file_path": distribution,
                "upload_url": config.upload_service_url,
            },
        )
