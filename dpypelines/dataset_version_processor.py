from pathlib import Path
from typing import Optional
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.config import JobConfiguration
from dpypelines.pipeline.dataset_api import validate_and_upload_metadata
from dpypelines.pipeline.messages.error_handler_module import error_handler
from dpypelines.pipeline.metadata.metadata_loader import MetadataLoader
from dpypelines.pipeline.models.metadata_models import Manifest, Metadata
from dpypelines.pipeline.process_zip_file import (
    copy_s3_processing_folder_to_destination_folder,
    delete_s3_processing_folder,
    process_zip_file,
)
from dpypelines.pipeline.utils import (
    send_submission_confirmation,
    setup_clients,
    upload_files,
)
from dpytools.stores.directory.local import LocalDirectoryStore
from dpypelines.pipeline.validate_pipeline import (
    validate_manifest,
)
from dpytools.http.api.dataset_api_service import DatasetAPIService

class ProcessedZipFile:
    def __init__(self, local_store: LocalDirectoryStore, decompressed_file_dir: Path, s3_processing_folder: str, s3_object_name: str, manifest: Manifest):
        self.local_store = local_store
        self.decompressed_file_dir = decompressed_file_dir
        self.s3_processing_folder = s3_processing_folder
        self.s3_object_name = s3_object_name
        self.manifest = manifest
        
class DatasetVersionProcessor:
    # Global boolean variables for error handler, to allow easy setting
    ENABLE_NOTIFICATION = True
    ENABLE_LOGS = True
    ENABLE_EMAIL = True

    def __init__(self):
        self.logger = DpLogger("data-ingress-pipeline")
        self.notifier, self.email_client = setup_clients()
        self.dataset_api_service = self.get_dataset_api_service()
        
    def process_s3_object_event(self, s3_object_name: str) -> bool:
        manifest = self.get_manifest(s3_object_name)
        if manifest is None:
            return False
        
        return self.process_manifest(manifest)
    
    def get_manifest(self, s3_object_name: str) -> Optional[ProcessedZipFile]:
        try:
            # Create local directory store from decompressed zip file and move files to S3 `processing` folder
            local_store, decompressed_file_dir, s3_processing_folder = process_zip_file(
                s3_object_name
            )

            # Validate configuration and files.
            manifest = validate_manifest(local_store)
            return ProcessedZipFile(
                local_store=local_store,
                decompressed_file_dir=decompressed_file_dir,
                s3_processing_folder=s3_processing_folder,
                s3_object_name=s3_object_name,
                manifest=manifest
            )
        except Exception as err:
            self.__handle_exception(s3_object_name=s3_object_name, submitter_email="", err=err)
            return None
            
    def process_manifest(self, processed_zip_file: ProcessedZipFile):
        try:
            metadata = MetadataLoader(self.dataset_api_service, self.logger).load_metadata(
                processed_zip_file.manifest, processed_zip_file.local_store
            )

            # Upload metadata to Dataset API.
            if  JobConfiguration().skip_data_upload:
                # todo: log
                return False
            
            metadata_submitted = validate_and_upload_metadata(
                metadata=metadata,
                dataset_api_service=self.dataset_api_service,
            )

            if not metadata_submitted:
                self.handle_failed_metadata_upload(processed_zip_file)
                return False

            self.handle_successful_metadata_upload(processed_zip_file, metadata)
            return True
        except Exception as err:
            self.__handle_exception(processed_zip_file.s3_object_name, submitter_email=processed_zip_file.manifest.get_submission_contact_email(), err=err)
            return False

    def __handle_exception(self, s3_object_name: str, submitter_email: str, err: Exception):
        self.logger.error("ETL process failed", err)
        error_handler(
                section="ETL",
                error=err,
                data={"s3_object_name": s3_object_name},
                submitter_email=submitter_email,
                enable_email=self.ENABLE_EMAIL,
                enable_logs=self.ENABLE_LOGS,
                enable_notification=self.ENABLE_NOTIFICATION,
                notifier=self.notifier,
            )

    def handle_successful_metadata_upload(self, processed_zip_file: ProcessedZipFile, metadata: Metadata):
        # If metadata successfully submitted, upload data files to the Upload Service
        files_to_upload = [
            processed_zip_file.decompressed_file_dir / distribution.file
            for distribution in metadata.distributions
        ]
        upload_files(files_to_upload)

        # Copy all files in S3 "processing" folder to S3 "processed" folder
        self.__copy_processing_to_destination(processed_zip_file, "processed")
        send_submission_confirmation(
            self.email_client, processed_zip_file.manifest.submission_contacts[0].email
        )
        self.notifier.success()
        self.logger.info("ETL process completed successfully")

        # Delete files from S3 "processing" folder
        delete_s3_processing_folder(
            processed_zip_file.s3_object_name, processed_zip_file.decompressed_file_dir, processed_zip_file.s3_processing_folder
        )

    def handle_failed_metadata_upload(self, processed_zip_file: ProcessedZipFile):
        # Copy all files in S3 "processing" folder to S3 "dataset-type-not-static" folder
        self.__copy_processing_to_destination(processed_zip_file, "failed-metadata-upload")
        # Delete files from S3 "processing" folder
        delete_s3_processing_folder(
            processed_zip_file.s3_object_name, processed_zip_file.decompressed_file_dir, processed_zip_file.s3_processing_folder
        )

    def get_dataset_api_service(self):
        dataset_api_url = JobConfiguration().dataset_api_url
        if not dataset_api_url:
            msg = "Required variable not set: DATASET_API_URL"
            raise EnvironmentError(msg)
        dataset_api_service = DatasetAPIService(dataset_api_url)
        return dataset_api_service


    def __copy_processing_to_destination(self, processed_zip_file: ProcessedZipFile, destination):
        copy_s3_processing_folder_to_destination_folder(
            processed_zip_file.s3_object_name,
            processed_zip_file.decompressed_file_dir,
            processed_zip_file.s3_processing_folder,
            destination
        )
