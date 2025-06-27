from faker import Faker
from dpypelines.pipeline.config.job_config import JobConfig
from tests.helpers.generators.dataset.generator import (
    DatasetGenerator,
    GeneratedDataset,
)
from tests.integration.helpers.file_helpers import (
    FileGenerationConfig,
    create_test_zip_file,
)
import boto3


faker = Faker()


class E2ETestBase:
    generator: DatasetGenerator

    def __init__(self, job_config: JobConfig, test_email_address: str):
        self.job_config = job_config
        self.generator = DatasetGenerator(
            dataset_api_url=job_config.dataset_api_url,
            upload_service_url=job_config.upload_service_url,
        )

        self.s3_client = boto3.client("s3")
        self.test_email_address = test_email_address

    def generate_test_dataset(self, publish: bool = False) -> GeneratedDataset:
        return self.generator.generate_complete_dataset(publish)

    def generate_and_upload_s3_file(self, dataset_id: str, edition_id: str):
        test_zip_file = create_test_zip_file(
            FileGenerationConfig(
                overrides={"submission_contacts": [{"email": self.test_email_address}]}
            ),
            FileGenerationConfig(
                overrides={"dataset_id": dataset_id, "edition": edition_id}
            ),
            FileGenerationConfig(),
            data_file_name="data.csv",
        )

        s3_bucket_name = f"dp-{self.job_config.environment}-ingest-submission-bucket"
        s3_object_key = f"testing/{str(test_zip_file.name)}"

        # TODO: add bucket name to config
        self.s3_client.upload_file(test_zip_file, s3_bucket_name, s3_object_key)

        return f"{s3_bucket_name}/{s3_object_key}"
