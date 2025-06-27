from pathlib import Path
from typing import Optional, Tuple
from dpytools.http.api import Dataset, DatasetType, DatasetVersion, DatasetState
from faker import Faker
from dpypelines.pipeline.config.job_config import get_job_config
from dpypelines.pipeline.models.metadata_models import Distribution
from tests.helpers.generators.dataset.models_generators import (
    generate_dataset,
    generate_dataset_version,
    generate_id,
)
from dpytools.http.api import DatasetAPIService
from dpytools.http.upload.upload_service_client import UploadServiceClient
from tests.helpers.generators.data_file_generators import generate_data_file
from dpytools.http.api.models.version import Distribution as DpytoolsDistribution

faker = Faker()


class GeneratedDataset:
    """
    GeneratedDataset contains the generated dataset information
    """

    dataset: Dataset
    edition_id: str
    version: DatasetVersion

    def __init__(self, dataset: Dataset, edition_id: str, version: DatasetVersion):
        self.dataset = dataset
        self.edition_id = edition_id
        self.version = version


class DatasetGenerator:
    """
    DatasetGenerator generates random datasets, files, and versions, and then creates them on the Dataset API + Upload Service.
    """

    dataset_service: DatasetAPIService
    upload_service: UploadServiceClient

    def __init__(self, dataset_api_url: str, upload_service_url: str):
        self.dataset_service = DatasetAPIService(dataset_api_url)
        self.upload_service = UploadServiceClient(upload_service_url)

    def generate_dataset(self) -> Dataset:
        """
        Generate a dataset, and create it in the Dataset API
        """
        dataset = generate_dataset(DatasetType.STATIC)

        result = self.dataset_service.datasets.create_dataset(dataset)

        return Dataset(**result)

    def generate_and_upload_file(self) -> Distribution:
        """
        Generate a random CSV file and upload it to the Upload Service. Returns a mapped Distribution instance.
        """
        format = "csv"
        file_path = f"./file.{format}"
        generate_data_file(Path(file_path))
        distribution = Distribution(
            title=faker.sentence(), format=format, file=file_path
        )
        self.upload_service.upload_new(
            distribution.file,
            distribution.media_type,
            upload_path=distribution.upload_path,
            identifier=distribution.identifier,
        )
        return distribution

    def generate_version(
        self, dataset_id: str, edition_id: Optional[str] = None
    ) -> Tuple[str, DatasetVersion]:
        """
        Generate a dataset version, including a random file using generate_and_upload_file,
        then creates it in the Dataset API under the specified dataset_id + edition_id.

        If edition_id is None, a random one will be generated.
        """
        distributions = [
            DpytoolsDistribution(**distribution.model_dump())
            for distribution in [self.generate_and_upload_file()]
        ]
        version = generate_dataset_version(distributions, edition_id)

        if not edition_id:
            edition_id = generate_id()

        model = version.model_dump(
            exclude_defaults=True, exclude_none=True, exclude_unset=True
        )
        response = self.dataset_service.versions.create_version(
            model, dataset_id, edition_id
        )

        if response.ok:
            json_data = response.json()
            return edition_id, DatasetVersion(**json_data)

        response.raise_for_status()
        return "", version

    def publish_version(
        self, dataset_id: str, edition_id: str, version: int | str
    ) -> bool:
        """
        Publish the specified version using the Dataset API.
        """
        result = self.dataset_service.versions.update_version_state(
            dataset_id, edition_id, version, DatasetState.PUBLISHED
        )

        return result.ok

    def generate_complete_dataset(self, publish: bool = False) -> GeneratedDataset:
        """
        Generates a dataset, dataset version, and a file for the version, then uploads them to the upload service + Dataset API.

        Will also publish the dataset version if `publish` is True
        """
        dataset = self.generate_dataset()
        edition_id, version = self.generate_version(dataset.id)

        if publish:
            self.publish_version(dataset.id, edition_id, version.version)

        return GeneratedDataset(dataset, edition_id, version)

    def generate_multiple_complete_datasets(self, count: int) -> list[GeneratedDataset]:
        """
        Generates + creates a number of datasets, dataset versions, and files for the versions.
        """
        return [self.generate_complete_dataset() for i in range(count)]


def example_dataset_generator_usage():
    """
    Example on how to use the DatasetGenerator
    """
    job_config = get_job_config()
    generator = DatasetGenerator(
        job_config.dataset_api_url, job_config.upload_service_url
    )
    datasets = generator.generate_multiple_complete_datasets(10)
    print(datasets)
