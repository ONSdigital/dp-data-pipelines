from typing import Optional

from bson.objectid import ObjectId
from dpytools.logging.logger import DpLogger

from dpypelines.pipeline.config.job_config import get_job_config
from dpypelines.pipeline.api.dataset_api import validate_and_upload_metadata
from dpypelines.pipeline.api.upload import upload_files
from dpypelines.pipeline.db.datasets_service import DatasetsService
from dpypelines.pipeline.db.db_collection_factories import DatasetsServiceFactory
import dpypelines.pipeline.db.db_models as models
from dpypelines.pipeline.db.db_model_factories import (
    DatasetEventDataFactory,
    DatasetEventFactory,
)
from dpypelines.pipeline.messages.notification import send_submission_confirmation
from dpypelines.pipeline.messages.error_handler import error_handler
from dpypelines.pipeline.metadata.metadata_loader import MetadataLoader
from dpypelines.pipeline.metadata.metadata_models import Metadata
from dpypelines.pipeline.process_zip_file import (
    ProcessedZipFile,
    process_zip_file,
    S3Object,
)
from dpypelines.pipeline.messages.setup import (
    setup_clients,
)
from dpypelines.pipeline.validate_pipeline import (
    validate_manifest,
)
from dpytools.http.upload.upload_service_client import UploadServiceClient
from dpytools.http.api.dataset_api_service import DatasetAPIService
from dpytools.db.documentdb_client import DocumentDBClientOptions, DocumentDBClient


class ETLProcessor:
    # Global boolean variables for error handler, to allow easy setting
    ENABLE_NOTIFICATION = True
    ENABLE_LOGS = True
    ENABLE_EMAIL = True

    def __init__(self, s3_object_name: str):
        self.job_config = get_job_config()
        self.notifier, self.email_client = setup_clients(self.job_config)
        self.logger = DpLogger("etl-processor")
        self.dataset_api_service = self.get_dataset_api_service()
        self.upload_service_client = self.get_upload_service_client()
        self.db_datasets_service = self.get_db_datasets_service()
        self.s3_object = S3Object(s3_object_name=s3_object_name)
        self.current_status = models.DatasetStatusType.PENDING
        self.submitter_email = ""

    def process_s3_object_event(self, status_oid: ObjectId) -> bool:
        try:
            processed_zip_file = self.get_processed_zip_file(status_oid)
            if processed_zip_file is None:
                return False
            return self.process_metadata_and_distributions(
                processed_zip_file, status_oid
            )
        except Exception as err:
            self.__handle_exception(
                s3_object_name=self.s3_object.name,
                status_oid=status_oid,
                err=err,
            )
            return False

    def get_processed_zip_file(
        self, status_oid: ObjectId
    ) -> Optional[ProcessedZipFile]:
        # Update datasets and statuses collections with processing event
        self.current_status = models.DatasetStatusType.PROCESSING
        updated_dataset = self.db_datasets_service.update_dataset_existing_status(
            dataset_id=self.s3_object.dataset_id,
            status_oid=status_oid,
            event=DatasetEventFactory.create_processing_dataset_event(
                dataset_id=self.s3_object.dataset_id,
                event_data=DatasetEventDataFactory.create_dataset_event_data(
                    s3_object_key=self.s3_object.key
                ),
            ),
            new_status=self.current_status,
        )
        self.logger.info(
            "Datasets collection updated",
            data={"dataset_id": updated_dataset.dataset_id},
        )

        # Create local directory store from decompressed zip file
        local_store, decompressed_file_dir = process_zip_file(self.s3_object)

        # Validate configuration and files.
        manifest = validate_manifest(local_store)
        self.submitter_email = manifest.get_submission_contact_email()

        return ProcessedZipFile(
            s3_object=self.s3_object,
            local_store=local_store,
            decompressed_file_dir=decompressed_file_dir,
            manifest=manifest,
        )

    def process_metadata_and_distributions(
        self, processed_zip_file: ProcessedZipFile, status_oid: ObjectId
    ) -> bool:
        # Load metadata
        metadata = MetadataLoader(self.dataset_api_service, self.logger).load_metadata(
            processed_zip_file.manifest, processed_zip_file.local_store
        )

        if self.job_config.skip_data_upload:
            # Update datasets and statuses collections with completion event if data upload skipped
            self.current_status = models.DatasetStatusType.COMPLETED
            updated_dataset = self.db_datasets_service.update_dataset_existing_status(
                dataset_id=self.s3_object.dataset_id,
                status_oid=status_oid,
                event=DatasetEventFactory.create_completed_dataset_event(
                    dataset_id=self.s3_object.dataset_id,
                    event_data=DatasetEventDataFactory.create_dataset_event_data(
                        s3_object_key=self.s3_object.key,
                        additional_data={"Info": "Data upload skipped"},
                    ),
                ),
                new_status=self.current_status,
            )
            return False

        # Upload distributions to Upload Service
        files_to_upload = [
            processed_zip_file.decompressed_file_dir / distribution.file
            for distribution in metadata.distributions
        ]
        upload_files(files_to_upload, self.job_config, self.upload_service_client)

        # Update datasets and statuses collections with upload event (Upload Service)
        updated_dataset = self.db_datasets_service.update_dataset_existing_status(
            dataset_id=self.s3_object.dataset_id,
            status_oid=status_oid,
            event=DatasetEventFactory.create_uploaded_dataset_event(
                dataset_id=self.s3_object.dataset_id,
                event_data=DatasetEventDataFactory.create_dataset_event_data(
                    s3_object_key=self.s3_object.key,
                    upload_location=models.UploadLocation.UPLOAD_SERVICE,
                ),
            ),
            new_status=self.current_status,
        )
        self.logger.info(
            "Datasets collection updated",
            data={"dataset_id": updated_dataset.dataset_id},
        )

        self.handle_successful_data_upload(metadata, status_oid)
        return True

    def handle_successful_data_upload(
        self,
        metadata: Metadata,
        status_oid: ObjectId,
    ):
        # Upload metadata to Dataset API.
        validate_and_upload_metadata(
            metadata=metadata,
            dataset_api_service=self.dataset_api_service,
        )

        # Update datasets and statuses collections with upload event (Dataset API)
        updated_dataset = self.db_datasets_service.update_dataset_existing_status(
            dataset_id=self.s3_object.dataset_id,
            status_oid=status_oid,
            event=DatasetEventFactory.create_uploaded_dataset_event(
                dataset_id=self.s3_object.dataset_id,
                event_data=DatasetEventDataFactory.create_dataset_event_data(
                    s3_object_key=self.s3_object.key,
                    upload_location=models.UploadLocation.DATASET_API,
                ),
            ),
            new_status=self.current_status,
        )
        self.logger.info(
            "Datasets collection updated",
            data={"dataset_id": updated_dataset.dataset_id},
        )

        # Update datasets and statuses collections with completion event
        self.current_status = models.DatasetStatusType.COMPLETED
        updated_dataset = self.db_datasets_service.update_dataset_existing_status(
            dataset_id=self.s3_object.dataset_id,
            status_oid=status_oid,
            event=DatasetEventFactory.create_completed_dataset_event(
                dataset_id=self.s3_object.dataset_id,
                event_data=DatasetEventDataFactory.create_dataset_event_data(
                    s3_object_key=self.s3_object.key,
                ),
            ),
            new_status=self.current_status,
        )
        self.logger.info("ETL process completed successfully")

        # Send success email and notification
        send_submission_confirmation(self.email_client, self.submitter_email)
        try:
            self.notifier.success()
        except Exception as err:
            # Prevent email being sent to submission contact if Slack notification fails
            self.ENABLE_EMAIL = False
            raise err

    def get_dataset_api_service(self):
        if not self.job_config.dataset_api_url:
            msg = "Required variable not set: DATASET_API_URL"
            raise EnvironmentError(msg)
        return DatasetAPIService(self.job_config.dataset_api_url)

    def get_upload_service_client(self):
        if not self.job_config.upload_service_url:
            msg = "Required variable not set: UPLOAD_SERVICE_URL"
            raise EnvironmentError(msg)
        return UploadServiceClient(self.job_config.upload_service_url)

    def get_db_datasets_service(
        self,
        host: Optional[str] = None,
        port: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
    ) -> DatasetsService:
        if self.job_config.database_connection_string:
            client_options = DocumentDBClientOptions(
                connection_string=self.job_config.database_connection_string
            )
        elif host and port and username and password:
            client_options = DocumentDBClientOptions(
                host=host,
                port=port,
                username=username,
                password=password,
                connection_string=connection_string,
            )
        else:
            client_options = DocumentDBClientOptions()

        client = DocumentDBClient(
            client_options=client_options, database_name=self.job_config.database_name
        )
        client.connect()
        db_datasets_service = DatasetsServiceFactory.create_db_datasets_service(
            client=client
        )
        return db_datasets_service

    def __handle_exception(
        self,
        s3_object_name: str,
        status_oid: ObjectId,
        err: Exception,
    ):
        self.logger.error("ETL process failed", err)
        # If status is COMPLETED, raise error but don't update DB
        if self.current_status != models.DatasetStatusType.COMPLETED:
            self.current_status = models.DatasetStatusType.FAILED
            self.db_datasets_service.update_dataset_existing_status(
                dataset_id=self.s3_object.dataset_id,
                status_oid=status_oid,
                event=DatasetEventFactory.create_failed_dataset_event(
                    dataset_id=self.s3_object.dataset_id,
                    event_data=DatasetEventDataFactory.create_dataset_event_data(
                        s3_object_key=self.s3_object.key,
                    ),
                    error_message=str(err),
                ),
                new_status=self.current_status,
            )
        error_handler(
            error=err,
            data={"s3_object_name": s3_object_name},
            submitter_email=self.submitter_email,
            enable_email=self.ENABLE_EMAIL,
            enable_logs=self.ENABLE_LOGS,
            enable_notification=self.ENABLE_NOTIFICATION,
            notifier=self.notifier,
            email_client=self.email_client,
        )
