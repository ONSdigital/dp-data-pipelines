from pathlib import Path
from unittest.mock import MagicMock, patch

import mongomock
from dpytools.stores.directory.local import LocalDirectoryStore
import pytest
from requests import HTTPError

from dpypelines.pipeline.errors import DatasetNotFoundException
from dpypelines.pipeline.etl_processor import ETLProcessor

from dpypelines.pipeline.metadata.metadata_models import (
    Distribution,
    Manifest,
    Metadata,
    SubmissionContact,
)
from dpypelines.pipeline.process_zip_file import ProcessedZipFile, S3Object


@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_etl_processor(
    mock_job_config,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
):
    mock_job_config.return_value.upload_service_url = "http://upload.url"
    mock_job_config.return_value.dataset_api_url = "http://datasetapi.url"
    mock_job_config.return_value.connection_string = "mongodb://connectionstring"
    mock_job_config.return_value.database_name = "statuses"
    mock_job_config.return_value.skip_data_upload = False

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    etl_processor = ETLProcessor(s3_object_name)
    assert isinstance(etl_processor, ETLProcessor)
    assert etl_processor.s3_object.name == s3_object_name
    mock_UploadServiceClient.assert_called_once_with("http://upload.url")
    mock_DatasetAPIService.assert_called_once_with("http://datasetapi.url")
    mock_DatasetsServiceFactory.assert_called_once_with(
        client=mock_DocumentDBClient.return_value
    )


@patch("dpypelines.pipeline.etl_processor.upload_files")
@patch("dpypelines.pipeline.etl_processor.validate_and_upload_metadata")
@patch("dpypelines.pipeline.etl_processor.MetadataLoader")
@patch("dpypelines.pipeline.etl_processor.validate_manifest")
@patch("dpypelines.pipeline.etl_processor.process_zip_file")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_s3_object_event_success(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_process_zip_file,
    mock_validate_manifest,
    mock_MetadataLoader,
    mock_validate_metadata,
    mock_upload_files,
    tmp_path,
):
    mock_job_config.return_value.upload_service_url = "http://upload.url"
    mock_job_config.return_value.dataset_api_url = "http://datasetapi.url"
    mock_job_config.return_value.skip_data_upload = False
    mock_job_config.return_value.database_name = "statuses"

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    local_store = LocalDirectoryStore(tmp_path)
    mock_process_zip_file.return_value = (local_store, "files")

    mock_validate_manifest.return_value = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
        use_previous_metadata=False,
    )
    metadata = Metadata(
        dataset_id="dataset_id_1",
        edition="edition_id",
        distributions=[],
        release_date="release_date",
    )
    mock_MetadataLoader.return_value.load_metadata.return_value = metadata
    mock_validate_metadata.return_value = True

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    s3_object_processed = etl_processor.process_s3_object_event(status_oid)

    assert s3_object_processed
    mock_process_zip_file.assert_called_once_with(etl_processor.s3_object)
    mock_validate_manifest.assert_called_once_with(local_store)

    mock_validate_metadata.assert_called_once_with(
        metadata=metadata,
        dataset_api_service=mock_dataset_api_service,
    )
    mock_upload_files.assert_called_once_with(
        [], mock_job_config.return_value, mock_upload_client
    )
    assert mock_datasets_service.update_dataset_existing_status.call_count == 4
    mock_notifier.success.assert_called_once()
    mock_email_client.send.assert_called_once()


@patch("dpypelines.pipeline.etl_processor.process_zip_file")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_s3_object_event_process_zip_fails(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_process_zip_file,
):
    mock_job_config.return_value.upload_service_url = "http://upload.url"
    mock_job_config.return_value.dataset_api_url = "http://datasetapi.url"
    mock_job_config.return_value.skip_data_upload = False
    mock_job_config.return_value.database_name = "statuses"

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    mock_process_zip_file.side_effect = FileNotFoundError(
        "Decompressed directory is empty for s3_object_key"
    )

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    with pytest.raises(FileNotFoundError) as e:
        etl_processor.process_s3_object_event(status_oid)
    assert "Decompressed directory is empty for s3_object_key" in str(e)


@patch("dpypelines.pipeline.etl_processor.validate_manifest")
@patch("dpypelines.pipeline.etl_processor.process_zip_file")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_s3_object_event_validate_manifest_fails(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_process_zip_file,
    mock_validate_manifest,
    tmp_path,
):
    mock_job_config.return_value.upload_service_url = "http://upload.url"
    mock_job_config.return_value.dataset_api_url = "http://datasetapi.url"
    mock_job_config.return_value.skip_data_upload = False
    mock_job_config.return_value.database_name = "statuses"

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    local_store = LocalDirectoryStore(tmp_path)
    mock_process_zip_file.return_value = (local_store, "files")

    mock_validate_manifest.side_effect = FileNotFoundError(
        "Failed to retrieve manifest from the local directory store."
    )

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    with pytest.raises(FileNotFoundError) as e:
        etl_processor.process_s3_object_event(status_oid)
    assert "Failed to retrieve manifest from the local directory store." in str(e)


@patch("dpypelines.pipeline.etl_processor.upload_files")
@patch("dpypelines.pipeline.etl_processor.validate_and_upload_metadata")
@patch("dpypelines.pipeline.etl_processor.MetadataLoader")
@patch("dpypelines.pipeline.etl_processor.validate_manifest")
@patch("dpypelines.pipeline.etl_processor.process_zip_file")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_s3_object_event_validate_metadata_fails(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_process_zip_file,
    mock_validate_manifest,
    mock_MetadataLoader,
    mock_validate_metadata,
    mock_upload_files,
    tmp_path,
):
    mock_job_config.return_value.upload_service_url = "http://upload.url"
    mock_job_config.return_value.dataset_api_url = "http://datasetapi.url"
    mock_job_config.return_value.skip_data_upload = False
    mock_job_config.return_value.database_name = "statuses"

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    local_store = LocalDirectoryStore(tmp_path)
    mock_process_zip_file.return_value = (local_store, "files")

    mock_validate_manifest.return_value = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
        use_previous_metadata=False,
    )
    metadata = Metadata(
        dataset_id="dataset_id_1",
        edition="edition_id",
        distributions=[],
        release_date="release_date",
    )
    mock_MetadataLoader.return_value.load_metadata.return_value = metadata
    mock_validate_metadata.side_effect = HTTPError

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    with pytest.raises(HTTPError) as e:
        etl_processor.process_s3_object_event(status_oid)
    mock_upload_files.assert_not_called()
    assert "HTTPError" in str(e)


@patch("dpypelines.pipeline.etl_processor.validate_manifest")
@patch("dpypelines.pipeline.etl_processor.process_zip_file")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_get_processed_zip_file_success(
    mock_job_config,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_process_zip_file,
    mock_validate_manifest,
    tmp_path,
):
    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    local_store = LocalDirectoryStore(tmp_path)
    mock_process_zip_file.return_value = (local_store, "files")

    manifest = Manifest(
        metadata_file="metadata.json",
        submission_contacts=[SubmissionContact(email="test@example.org")],
        use_previous_metadata=False,
    )
    mock_validate_manifest.return_value = manifest

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    processed_zip_file = etl_processor.get_processed_zip_file(status_oid)
    assert True
    assert isinstance(processed_zip_file, ProcessedZipFile)
    assert processed_zip_file.local_store == local_store
    assert processed_zip_file.manifest == manifest
    assert processed_zip_file.s3_object.name == s3_object_name


@patch("dpypelines.pipeline.etl_processor.validate_and_upload_metadata")
@patch("dpypelines.pipeline.etl_processor.MetadataLoader")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_metadata_success(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_MetadataLoader,
    mock_validate_metadata,
    tmp_path,
):
    mock_job_config.return_value.skip_data_upload = False

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    metadata = Metadata(
        dataset_id="dataset_id_1",
        edition="edition_id",
        distributions=[],
        release_date="release_date",
    )
    mock_MetadataLoader.return_value.load_metadata.return_value = metadata
    mock_validate_metadata.return_value = True

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    processed_zip_file = ProcessedZipFile(
        S3Object(s3_object_name),
        LocalDirectoryStore(tmp_path),
        Path("files"),
        Manifest(
            metadata_file="metadata.json",
            submission_contacts=[SubmissionContact(email="test@example.org")],
            use_previous_metadata=False,
        ),
    )

    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    metadata_processed = etl_processor.process_metadata(processed_zip_file, status_oid)
    mock_MetadataLoader.return_value.load_metadata.assert_called_once_with(
        processed_zip_file.manifest, processed_zip_file.local_store
    )
    mock_validate_metadata.assert_called_once_with(
        metadata=metadata,
        dataset_api_service=mock_dataset_api_service,
    )
    assert metadata_processed


@patch("dpypelines.pipeline.etl_processor.MetadataLoader")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_metadata_fails_metadata_loader(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_MetadataLoader,
    tmp_path,
):
    mock_job_config.return_value.skip_data_upload = False

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    mock_MetadataLoader.return_value.load_metadata.side_effect = ValueError(
        "File is empty"
    )

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    processed_zip_file = ProcessedZipFile(
        S3Object(s3_object_name),
        LocalDirectoryStore(tmp_path),
        Path("files"),
        Manifest(
            metadata_file="metadata.json",
            submission_contacts=[SubmissionContact(email="test@example.org")],
            use_previous_metadata=False,
        ),
    )

    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    with pytest.raises(ValueError) as e:
        etl_processor.process_metadata(processed_zip_file, status_oid)
    assert "File is empty" in str(e)


@patch("dpypelines.pipeline.etl_processor.validate_and_upload_metadata")
@patch("dpypelines.pipeline.etl_processor.MetadataLoader")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_process_metadata_fails_metadata_validation(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_MetadataLoader,
    mock_validate_metadata,
    tmp_path,
):
    mock_job_config.return_value.skip_data_upload = False

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    metadata = Metadata(
        dataset_id="dataset_id_1",
        edition="edition_id",
        distributions=[],
        release_date="release_date",
    )
    mock_MetadataLoader.return_value.load_metadata.return_value = metadata
    mock_validate_metadata.side_effect = DatasetNotFoundException(
        "dataset_id_1", "dataset_path"
    )

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    processed_zip_file = ProcessedZipFile(
        S3Object(s3_object_name),
        LocalDirectoryStore(tmp_path),
        Path("files"),
        Manifest(
            metadata_file="metadata.json",
            submission_contacts=[SubmissionContact(email="test@example.org")],
            use_previous_metadata=False,
        ),
    )

    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    with pytest.raises(DatasetNotFoundException) as e:
        etl_processor.process_metadata(processed_zip_file, status_oid)
    assert "Dataset dataset_id_1 not found" in str(e)


@patch("dpypelines.pipeline.etl_processor.upload_files")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_handle_successful_metadata_upload_success(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_upload_files,
    tmp_path,
):
    mock_job_config.return_value.skip_data_upload = False

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    processed_zip_file = ProcessedZipFile(
        S3Object(s3_object_name),
        LocalDirectoryStore(tmp_path),
        Path("files"),
        Manifest(
            metadata_file="metadata.json",
            submission_contacts=[SubmissionContact(email="test@example.org")],
            use_previous_metadata=False,
        ),
    )
    metadata = Metadata(
        dataset_id="dataset_id_1",
        edition="edition_id",
        distributions=[
            Distribution(title="Distribution", format="csv", file="data.csv")
        ],
        release_date="release_date",
    )
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    etl_processor.handle_successful_metadata_upload(
        processed_zip_file, metadata, status_oid
    )
    mock_upload_files.assert_called_once_with(
        [Path("files/data.csv")], mock_job_config.return_value, mock_upload_client
    )
    mock_email_client.send.assert_called_once()
    mock_notifier.success.assert_called_once()
    assert mock_datasets_service.update_dataset_existing_status.call_count == 2


@patch("dpypelines.pipeline.etl_processor.upload_files")
@patch("dpypelines.pipeline.etl_processor.DocumentDBClient")
@patch(
    "dpypelines.pipeline.etl_processor.DatasetsServiceFactory.create_db_datasets_service"
)
@patch("dpypelines.pipeline.etl_processor.UploadServiceClient")
@patch("dpypelines.pipeline.etl_processor.DatasetAPIService")
@patch("dpypelines.pipeline.etl_processor.setup_clients")
@patch("dpypelines.pipeline.etl_processor.get_job_config")
def test_handle_successful_metadata_upload_fails_upload(
    mock_job_config,
    mock_setup_clients,
    mock_DatasetAPIService,
    mock_UploadServiceClient,
    mock_DatasetsServiceFactory,
    mock_DocumentDBClient,
    mock_upload_files,
    tmp_path,
):
    mock_job_config.return_value.skip_data_upload = False

    mock_notifier, mock_email_client = (
        MagicMock(name="notifier"),
        MagicMock(name="email_client"),
    )
    mock_setup_clients.return_value = (mock_notifier, mock_email_client)

    mock_dataset_api_service = MagicMock(name="DatasetAPIService")
    mock_DatasetAPIService.return_value = mock_dataset_api_service
    mock_upload_client = MagicMock(name="UploadServiceClient")
    mock_UploadServiceClient.return_value = mock_upload_client
    mock_datasets_service = MagicMock(name="DatasetsService")
    mock_DatasetsServiceFactory.return_value = mock_datasets_service
    mock_mongo_client = mongomock.MongoClient()
    mock_DocumentDBClient.connect.return_value = mock_mongo_client

    mock_upload_files.side_effect = HTTPError

    s3_object_name = "bucket/input/dataset_id_1 - test.zip"
    processed_zip_file = ProcessedZipFile(
        S3Object(s3_object_name),
        LocalDirectoryStore(tmp_path),
        Path("files"),
        Manifest(
            metadata_file="metadata.json",
            submission_contacts=[SubmissionContact(email="test@example.org")],
            use_previous_metadata=False,
        ),
    )
    metadata = Metadata(
        dataset_id="dataset_id_1",
        edition="edition_id",
        distributions=[
            Distribution(title="Distribution", format="csv", file="data.csv")
        ],
        release_date="release_date",
    )
    status_oid = mongomock.ObjectId()
    etl_processor = ETLProcessor(s3_object_name)
    with pytest.raises(HTTPError) as e:
        etl_processor.handle_successful_metadata_upload(
            processed_zip_file, metadata, status_oid
        )
    assert "HTTPError" in str(e)
