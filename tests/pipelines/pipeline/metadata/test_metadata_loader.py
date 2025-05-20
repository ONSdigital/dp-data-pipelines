import pytest
from unittest.mock import Mock, patch, call
from pathlib import Path
from dpytools.http.api.versions.dataset_versions_service import DatasetVersionsService
from dpypelines.pipeline.models.metadata_models import (
    Distribution,
    Manifest,
    Metadata,
    MinimalMetadata,
)
from dpytools.stores.directory.local import LocalDirectoryStore
from dpytools.http.api.dataset_api_service import DatasetAPIService
from dpytools.http.api.models.version import DatasetVersion
from dpytools.logging.logger import DpLogger
from dpypelines.pipeline.metadata.metadata_loader import MetadataLoader


class TestMetadataLoader:
    @pytest.fixture
    def mock_existing_metadata_dict(self):
        return {
            "dataset_id": "test-dataset",
            "edition": "test-edition",
            "quality_designation": "original-quality-designation",
            "edition_title": "Original edition title",
            "release_date": "2025-05-05",
            "state": "published",
        }

    @pytest.fixture
    def mock_dataset_api_latest_version(self, mock_existing_metadata_dict):
        return DatasetVersion(**mock_existing_metadata_dict)

    @pytest.fixture
    def mock_dataset_api_service(self, mock_dataset_api_latest_version):
        mock_dataset_api_service_cls = Mock(spec=DatasetAPIService)
        mock_dataset_api_service_cls.versions = Mock(spec=DatasetVersionsService)
        mock_versions_response = Mock()
        mock_versions_response.items = [Mock(), Mock()]
        mock_versions_response.get_latest_version.return_value = (
            mock_dataset_api_latest_version
        )

        mock_dataset_api_service_cls.versions.get_versions.return_value = (
            mock_versions_response
        )
        return mock_dataset_api_service_cls

    @pytest.fixture
    def mock_logger(self):
        return Mock(spec=DpLogger)

    @pytest.fixture
    def metadata_loader(self, mock_dataset_api_service, mock_logger):
        return MetadataLoader(mock_dataset_api_service, mock_logger)

    @pytest.fixture
    def mock_local_store(self):
        store = Mock(spec=LocalDirectoryStore)
        store.local_path = Mock()
        store.local_path.absolute.return_value = Path("/test/path")
        return store

    @pytest.fixture
    def mock_manifest(self):
        manifest = Mock(spec=Manifest)
        manifest.metadata_file = "metadata.json"
        manifest.use_previous_metadata = False
        return manifest

    @pytest.fixture
    def mock_manifest_with_previous_metadata(self):
        manifest = Mock(spec=Manifest)
        manifest.metadata_file = "metadata.json"
        manifest.use_previous_metadata = True
        return manifest

    @pytest.fixture
    def mock_metadata_dict(self):
        return {
            "dataset_id": "test-dataset",
            "edition": "test-edition",
            "quality_designation": "original",
            "edition_title": "Test Dataset",
            "release_date": "2020-01-01",
            "distributions": [
                {"file": "data.csv", "format": "csv", "title": "Test Data"}
            ],
        }

    @pytest.fixture
    def mock_distribution(self):
        return Distribution(file="data.csv", format="csv", title="Test Data")

    @pytest.fixture
    def mock_metadata(self, mock_distribution):
        return Metadata(
            dataset_id="test-dataset",
            edition="test-edition",
            distributions=[mock_distribution],
            release_date="2025-05-22",
        )

    def test_init(self, mock_dataset_api_service, mock_logger):
        """Test MetadataLoader initialization."""
        loader = MetadataLoader(mock_dataset_api_service, mock_logger)

        assert loader.dataset_api_service == mock_dataset_api_service
        assert loader.logger == mock_logger

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    @patch("dpypelines.pipeline.metadata.metadata_loader.read_json_file")
    def test_load_metadata_without_previous_metadata_success(
        self,
        mock_read_json_file,
        mock_validate_file,
        metadata_loader,
        mock_manifest,
        mock_local_store,
        mock_metadata_dict,
    ):
        """Test successful metadata loading without using previous metadata."""
        mock_read_json_file.return_value = mock_metadata_dict

        with patch.object(
            metadata_loader, "validate_distributions"
        ) as mock_validate_distributions:
            result = metadata_loader.load_metadata(mock_manifest, mock_local_store)

            mock_validate_file.assert_called_once_with(
                Path("/test/path") / "metadata.json"
            )
            mock_read_json_file.assert_called_once_with(
                Path("/test/path") / "metadata.json"
            )
            mock_validate_distributions.assert_called_once()

            assert isinstance(result, Metadata)
            assert result.dataset_id == "test-dataset"

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    @patch("dpypelines.pipeline.metadata.metadata_loader.read_json_file")
    def test_load_metadata_with_previous_metadata_success(
        self,
        mock_read_json_file,
        mock_validate_file,
        metadata_loader,
        mock_manifest_with_previous_metadata,
        mock_local_store,
        mock_metadata_dict,
        mock_dataset_api_latest_version,
    ):
        """Test successful metadata loading using previous metadata."""
        # Setup mocks
        mock_read_json_file.return_value = mock_metadata_dict

        with patch.object(
            metadata_loader, "_MetadataLoader__combine_metadata"
        ) as mock_get_combined:
            with patch.object(
                metadata_loader, "validate_distributions"
            ) as mock_validate_distributions:
                mock_combined_metadata = Mock(spec=Metadata)
                mock_get_combined.return_value = mock_combined_metadata

                result = metadata_loader.load_metadata(
                    mock_manifest_with_previous_metadata, mock_local_store
                )

                mock_validate_file.assert_called_once()
                mock_read_json_file.assert_called_once()
                mock_get_combined.assert_called_once_with(
                    mock_metadata_dict, mock_dataset_api_latest_version
                )
                mock_validate_distributions.assert_called_once_with(
                    mock_combined_metadata, mock_local_store
                )

                assert result == mock_combined_metadata

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    @patch("dpypelines.pipeline.metadata.metadata_loader.read_json_file")
    def test_load_metadata_from_file_success(
        self,
        mock_read_json_file,
        mock_validate_file,
        metadata_loader,
        mock_local_store,
        mock_metadata_dict,
    ):
        """Test successful metadata loading from file."""
        mock_read_json_file.return_value = mock_metadata_dict

        result = metadata_loader._MetadataLoader__load_metadata_from_file(
            "metadata.json", mock_local_store
        )

        mock_validate_file.assert_called_once_with(Path("/test/path") / "metadata.json")
        mock_read_json_file.assert_called_once_with(
            Path("/test/path") / "metadata.json"
        )
        assert result == mock_metadata_dict

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    def test_load_metadata_from_file_validation_failure(
        self, mock_validate_file, metadata_loader, mock_local_store
    ):
        """Test metadata loading failure when file validation fails."""
        mock_validate_file.side_effect = ValueError("File does not exist")

        with pytest.raises(ValueError, match="File does not exist"):
            metadata_loader._MetadataLoader__load_metadata_from_file(
                "metadata.json", mock_local_store
            )

    def test_get_combined_metadata_success(
        self, metadata_loader, mock_metadata_dict, mock_dataset_api_latest_version
    ):
        """Test successful combination of metadata."""
        result = metadata_loader._MetadataLoader__combine_metadata(
            mock_metadata_dict, mock_dataset_api_latest_version
        )

        assert isinstance(result, Metadata)
        assert result.edition_title == mock_metadata_dict["edition_title"]
        assert result.quality_designation == mock_metadata_dict["quality_designation"]

    def test_get_latest_version_metadata_from_api_success(
        self, metadata_loader, mock_dataset_api_service
    ):
        """Test successful retrieval of latest version metadata from API."""
        minimal_metadata = MinimalMetadata(
            dataset_id="test-dataset",
            edition="test-edition",
            distributions=[],
            release_date="2045-05-05",
        )

        mock_versions_response = Mock()
        mock_versions_response.items = [Mock(), Mock()]
        mock_latest_version = Mock(spec=DatasetVersion)
        mock_versions_response.get_latest_version.return_value = mock_latest_version

        mock_dataset_api_service.versions.get_versions.return_value = (
            mock_versions_response
        )

        result = metadata_loader._MetadataLoader__get_latest_version_metadata_from_api(
            minimal_metadata
        )

        mock_dataset_api_service.versions.get_versions.assert_called_once_with(
            dataset_id="test-dataset", edition_id="test-edition"
        )
        assert result == mock_latest_version

    def test_get_latest_version_metadata_from_api_no_versions(
        self, metadata_loader, mock_dataset_api_service
    ):
        """Test API call when no versions exist."""
        minimal_metadata = MinimalMetadata(
            dataset_id="test-dataset",
            edition="test-edition",
            distributions=[],
            release_date="2045-05-05",
        )

        mock_versions_response = Mock()
        mock_versions_response.items = []
        mock_dataset_api_service.versions.get_versions.return_value = (
            mock_versions_response
        )

        with pytest.raises(ValueError, match="Could not find existing version"):
            metadata_loader._MetadataLoader__get_latest_version_metadata_from_api(
                minimal_metadata
            )

    def test_get_latest_version_metadata_from_api_no_latest_version(
        self, metadata_loader, mock_dataset_api_service
    ):
        """Test API call when no latest version can be determined."""
        minimal_metadata = MinimalMetadata(
            dataset_id="test-dataset",
            edition="test-edition",
            distributions=[],
            release_date="2045-05-05",
        )

        mock_versions_response = Mock()
        mock_versions_response.items = [Mock()]
        mock_versions_response.get_latest_version.return_value = None
        mock_dataset_api_service.versions.get_versions.return_value = (
            mock_versions_response
        )

        with pytest.raises(ValueError, match="Could not find latest version"):
            metadata_loader._MetadataLoader__get_latest_version_metadata_from_api(
                minimal_metadata
            )

    def test_validate_distributions_success(
        self, metadata_loader, mock_metadata, mock_local_store, mock_logger
    ):
        """Test successful validation of all distributions."""
        with patch.object(
            metadata_loader, "validate_distribution_file"
        ) as mock_validate_dist:
            # Execute
            metadata_loader.validate_distributions(mock_metadata, mock_local_store)

            # Assertions
            mock_validate_dist.assert_called_once_with(
                mock_metadata.distributions[0], mock_local_store
            )
            mock_logger.info.assert_called_with("Metadata and data files validated.")

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    @patch("dpypelines.pipeline.metadata.metadata_loader.validate_file_format")
    def test_validate_distribution_file_success(
        self,
        mock_validate_format,
        mock_validate_exists,
        metadata_loader,
        mock_distribution,
        mock_local_store,
        mock_logger,
    ):
        """Test successful validation of a distribution file."""
        mock_validation_result = Mock()
        mock_validation_result.valid = True
        mock_validation_result.error = None
        mock_validation_result.format = "csv"
        mock_validate_format.return_value = mock_validation_result

        # Execute
        metadata_loader.validate_distribution_file(mock_distribution, mock_local_store)

        # Assertions
        mock_validate_exists.assert_called_once_with(Path("/test/path") / "data.csv")
        mock_validate_format.assert_called_once_with(Path("/test/path") / "data.csv")

        expected_calls = [
            call(f"Validating distribution file {mock_distribution.file}"),
            call("Data file found", data={"data_file_name": mock_distribution.file}),
            call(
                f"Validated file format for {mock_distribution.file}",
                data={"format": "csv"},
            ),
        ]
        mock_logger.info.assert_has_calls(expected_calls)

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    def test_validate_distribution_file_not_exists(
        self, mock_validate_exists, metadata_loader, mock_distribution, mock_local_store
    ):
        """Test validation failure when distribution file doesn't exist."""
        error_message = "File does not exist"
        mock_validate_exists.side_effect = ValueError(error_message)

        with pytest.raises(ValueError, match=error_message):
            metadata_loader.validate_distribution_file(
                mock_distribution, mock_local_store
            )

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    @patch("dpypelines.pipeline.metadata.metadata_loader.validate_file_format")
    def test_validate_distribution_file_invalid_format(
        self,
        mock_validate_format,
        mock_validate_exists,
        metadata_loader,
        mock_distribution,
        mock_local_store,
    ):
        """Test validation failure when file format is invalid."""
        mock_validation_result = Mock()
        mock_validation_result.valid = False
        mock_validation_result.error = "Invalid CSV format"
        mock_validate_format.return_value = mock_validation_result

        with pytest.raises(
            ValueError,
            match=f"File format validation failed for data.csv: {mock_validation_result.error}",
        ):
            metadata_loader.validate_distribution_file(
                mock_distribution, mock_local_store
            )

    @patch(
        "dpypelines.pipeline.metadata.metadata_loader.validate_file_exists_and_not_empty"
    )
    @patch("dpypelines.pipeline.metadata.metadata_loader.validate_file_format")
    def test_validate_distribution_file_format_error(
        self,
        mock_validate_format,
        mock_validate_exists,
        metadata_loader,
        mock_distribution,
        mock_local_store,
    ):
        """Test validation failure when format validation has error even if valid=True."""
        mock_validation_result = Mock()
        mock_validation_result.valid = True
        mock_validation_result.error = "Some warning error"
        mock_validate_format.return_value = mock_validation_result

        with pytest.raises(
            ValueError,
            match=f"File format validation failed for data.csv: {mock_validation_result.error}",
        ):
            metadata_loader.validate_distribution_file(
                mock_distribution, mock_local_store
            )
