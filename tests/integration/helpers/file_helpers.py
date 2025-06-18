import json
from pathlib import Path
import tempfile
from typing import Callable, Optional
import zipfile

from dpypelines.pipeline.models.metadata_models import QualityDesignation
from tests.helpers.generators.data_file_generators import (
    generate_data_file,
    get_media_type_for_extension,
)
from tests.integration.constants import (
    test_dataset_id,
    test_edition_id,
    MockDistributionDateTimeValue,
)

MANIFEST_FILE_NAME = "manifest.json"
METADATA_FILE_NAME = "metadata.json"
DATA_FILE_NAME = "data.csv"

FILE_AUTHOR_EMAIL = "test@example.com"


class FileGenerationConfig:
    """
    Model for configuring file generations for integrationtests
    """

    include: bool
    invalid: bool
    empty: bool
    missing_field_keys: list
    content: Optional[str]

    def __init__(
        self,
        include: bool = True,
        invalid: bool = False,
        empty: bool = False,
        missing_field_keys: list = [],
        content: Optional[str] = None,
    ):
        """
        Args:
            include: Include the file this config is for
            invalid: Generate an invalid file for this config
            empty: Generate an empty file for this config
            missing_field_keys: Remove these keys from the generated data
            content: Overwrite the generation with this specific file content
        """
        self.include = include
        self.invalid = invalid
        self.empty = empty
        self.missing_field_keys = missing_field_keys
        self.content = content


def write_json_file(temp_path: Path, contents: dict, file_name: str):
    with open(temp_path / file_name, "w") as f:
        json.dump(contents, f)


def write_text_file(temp_path: Path, file_name: str, contents: str):
    with open(temp_path / file_name, "w") as f:
        f.write(contents)


def write_empty_file(temp_path: Path, file_name: str):
    write_text_file(temp_path, file_name, "")


def process_config_overrides(
    file_name: str, temp_path: Path, config: FileGenerationConfig
):
    """
    Process specific overrides from the FileGenerationConfig, e.g. writing an empty file, or a file with content overrides

    Args:
        file_name: output file name
        temp_path: Target destination
        config: Config file file generation

    Returns:
        boolean indicating that we have written a file or not.
    """
    if config.empty:
        write_empty_file(temp_path, file_name)
        return True

    if config.content:
        write_text_file(temp_path, file_name, config.content)
        return True

    return False


def file_generator(file_name: str, data_dict_generator: Callable[[], dict]):
    def create_file(temp_path: Path, config: FileGenerationConfig):
        if process_config_overrides(file_name, temp_path, config):
            return

        dict_contents = data_dict_generator()

        for key in config.missing_field_keys:
            dict_contents.pop(key)

        write_json_file(temp_path, dict_contents, file_name)
        return dict_contents

    return create_file


def generate_manifest_dict():
    return {
        "submission_contacts": [
            {"email": FILE_AUTHOR_EMAIL},
        ],
        "metadata_file": "metadata.json",
        "use_previous_metadata": True,
    }


def create_manifest(temp_path: Path, config: FileGenerationConfig):
    return file_generator(MANIFEST_FILE_NAME, generate_manifest_dict)(temp_path, config)


def generate_distribution_for_file_name(file_name: str) -> dict:
    file_extension = file_name.split(".")[1]
    file_identifier = f"{MockDistributionDateTimeValue}-{file_name.replace(' ', '_').replace('.', '-')}"
    return {
        "title": "CSV Distribution",
        "download_url": f"/datasets/{file_identifier}/{file_name}",
        "file": file_name,
        "media_type": get_media_type_for_extension(file_extension),
        "format": file_extension,
    }


def generate_metadata_dict(data_file_name: str) -> dict:
    distribution = generate_distribution_for_file_name(data_file_name)
    return {
        "dataset_id": test_dataset_id,
        "edition": test_edition_id,
        "edition_title": "Edition title",
        "release_date": "2025-05-01T14:01:00",
        "distributions": [distribution],
        "alerts": [{"type": "alert type", "description": "some alert"}],
        "usage_notes": [{"title": "how to use me", "note": "read"}],
        "quality_designation": QualityDesignation.AccreditedOfficial.value,
    }


def create_metadata(temp_path: Path, config: FileGenerationConfig, data_file_name: str):
    def generate_metadata_for_data_file():
        return generate_metadata_dict(data_file_name)

    return file_generator(METADATA_FILE_NAME, generate_metadata_for_data_file)(
        temp_path, config
    )


def create_data_file(temp_path: Path, file_name: str, config: FileGenerationConfig):
    if process_config_overrides(file_name, temp_path, config):
        return

    combined_path = temp_path / file_name
    generate_data_file(combined_path)


def create_zip(temp_path):
    zip_path = Path(tempfile.mkstemp(suffix=".zip")[1])
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for file_path in temp_path.iterdir():
            zipf.write(file_path, arcname=file_path.name)
    return zip_path


def create_test_zip_file(
    manifest_config: FileGenerationConfig,
    metadata_config: FileGenerationConfig,
    data_config: FileGenerationConfig,
    data_file_name: str,
):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        if manifest_config.include:
            create_manifest(temp_path, manifest_config)

        if metadata_config.include:
            create_metadata(temp_path, metadata_config, data_file_name)

        if data_config.include:
            create_data_file(temp_path, data_file_name, data_config)

        zip_path = create_zip(temp_path)

        return zip_path
