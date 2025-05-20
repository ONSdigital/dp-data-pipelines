from typing import Dict, Any
from dpypelines.pipeline.models.metadata_models import (
    Distribution,
    Manifest,
    Metadata,
    MinimalMetadata,
)
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.http.api.dataset_api_service import DatasetAPIService
from dpytools.http.api.models.version import DatasetVersion
from dpypelines.pipeline.shared.files.json_file_readers import read_json_file
from dpytools.logging.logger import DpLogger
from dpypelines.pipeline.validation.utils import (
    validate_file_format,
    validate_file_exists_and_not_empty,
)


class MetadataLoader:
    def __init__(self, dataset_api_service: DatasetAPIService, logger: DpLogger):
        self.dataset_api_service = dataset_api_service
        self.logger = logger

    def load_metadata(
        self, manifest: Manifest, local_store: LocalDirectoryStore
    ) -> Metadata:
        """
        Load metadata from file, retrieve latest version from API, combine if necessary, and validate distributions files.
        """
        metadata_dict = self.__load_metadata_from_file(
            manifest.metadata_file, local_store
        )
        minimal_metadata = MinimalMetadata(**metadata_dict)
        latest_version = self.__get_latest_version_metadata_from_api(minimal_metadata)

        metadata = (
            Metadata(**metadata_dict)
            if not manifest.use_previous_metadata
            else self.__combine_metadata(metadata_dict, latest_version)
        )

        self.validate_distributions(metadata, local_store)
        return metadata

    def __load_metadata_from_file(
        self, metadata_file: str, local_store: LocalDirectoryStore
    ) -> dict:
        """
        Validate the manifest file and then reads + deserialises JSON to Metadata instance.
        """
        validate_file_exists_and_not_empty(
            local_store.local_path.absolute() / metadata_file
        )

        metadata_dict = read_json_file(
            local_store.local_path.absolute() / metadata_file
        )

        return metadata_dict

    def __combine_metadata(
        self, metadata_file_dict: Dict[str, Any], existing_metadata: DatasetVersion
    ) -> Metadata:
        """
        Get latest metadata from the Dataset API, then overwrite with any values from the metadata file.

        Args:
            metadata_file_dict: JSON from the metadata file that was uploaded
        """
        combined_metadata = existing_metadata.model_dump()
        combined_metadata.update(metadata_file_dict)
        metadata = Metadata(**combined_metadata)
        return metadata

    def __get_latest_version_metadata_from_api(
        self, minimal_metadata: MinimalMetadata
    ) -> DatasetVersion:
        """
        Get the latest version of the Dataset edition from the Dataset API

        Args:
            minimal_metadata: The minimal metadata we require to retrieve the latest version from the Dataset API
        """
        existing_metadata = self.dataset_api_service.versions.get_versions(
            dataset_id=minimal_metadata.dataset_id, edition_id=minimal_metadata.edition
        )

        if not existing_metadata.items or len(existing_metadata.items) == 0:
            raise ValueError(
                f"Could not find existing version for dataset {minimal_metadata.dataset_id}, edition {minimal_metadata.edition}"
            )

        latest_version = existing_metadata.get_latest_version()
        if latest_version is None:
            raise ValueError(
                f"Could not find latest version for dataset {minimal_metadata.dataset_id}, edition {minimal_metadata.edition}"
            )

        return latest_version

    def validate_distributions(
        self, metadata: Metadata, local_store: LocalDirectoryStore
    ):
        for distribution in metadata.distributions:
            self.validate_distribution_file(distribution, local_store)

        self.logger.info("Metadata and data files validated.")

    def validate_distribution_file(
        self, distribution: Distribution, local_store: LocalDirectoryStore
    ) -> None:
        self.logger.info(f"Validating distribution file {distribution.file}")
        file_path = local_store.local_path.absolute() / distribution.file

        validate_file_exists_and_not_empty(file_path)
        self.logger.info("Data file found", data={"data_file_name": distribution.file})

        validation_result = validate_file_format(file_path)
        if not validation_result.valid or validation_result.error:
            raise ValueError(
                f"File format validation failed for {distribution.file}: {validation_result.error}"
            )

        self.logger.info(
            f"Validated file format for {distribution.file}",
            data={"format": validation_result.format},
        )
