import os
import zipfile
from pathlib import Path
from typing import Tuple

from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import _get_s3_client
from dpytools.stores.directory.local import LocalDirectoryStore

from dpypelines.pipeline.metadata.metadata_models import Manifest


logger = DpLogger("data-ingress-pipelines")


class S3Object:
    def __init__(self, s3_object_name: str):
        parts = s3_object_name.split("/")
        self.name = s3_object_name
        self.bucket = parts[0]
        self.key = "/".join(parts[1:])
        self.filename = parts[-1]
        self.extension = parts[-1].split(".")[-1]
        self.folder = "/".join(parts[1:-1])
        if "-" in parts[-1]:
            self.dataset_id = parts[-1].split(" - ")[0]
        else:
            self.dataset_id = parts[-1].split(".")[0]

    def is_zip_file(self):
        return self.extension == "zip"


class ProcessedZipFile:
    def __init__(
        self,
        s3_object: S3Object,
        local_store: LocalDirectoryStore,
        decompressed_file_dir: Path,
        manifest: Manifest,
    ):
        self.s3_object = s3_object
        self.local_store = local_store
        self.decompressed_file_dir = decompressed_file_dir
        self.manifest = manifest


def download_zip_file(s3_object: S3Object) -> str:
    """
    Downloads a zip file from S3 into a local directory and returns the local file path.
    """
    input_dir, input_zip_name = s3_object.folder, s3_object.filename
    # Create a local directory to store downloaded zip file
    Path(f"/tmp/{input_dir}").mkdir(parents=True, exist_ok=True)

    target_path = f"/tmp/{s3_object.key}"
    logger.info(f'Changed object key from "{s3_object.key}" to "{target_path}"')

    # Download S3 object to local directory
    client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))
    with open(target_path, "wb") as f:
        client.download_fileobj(Bucket=s3_object.bucket, Key=s3_object.key, Fileobj=f)
    logger.info(
        "Downloaded zip file to local folder",
        data={"local_input_folder": input_dir, "local_file_name": input_zip_name},
    )

    return target_path


def decompress_zip_file(local_zip_path: str) -> Path:
    """
    Decompress the given zip file into a local folder named after the input zip file (without extension).
    """
    # Create destination directory to store decompressed files
    _, file_name = local_zip_path.rsplit("/", maxsplit=1)

    output_file_path = f"/tmp/{file_name.split('.')[0]}"
    destination_dir = Path(output_file_path)
    destination_dir.mkdir(parents=True, exist_ok=True)

    # Extract zip file to destination directory
    with zipfile.ZipFile(local_zip_path, "r") as f:
        f.extractall(destination_dir)

    # Handle different zip methods (zip files behave differently depending on operating system etc)
    if any([f.is_dir() for f in destination_dir.rglob("*")]):
        decompressed_file_dir = destination_dir / destination_dir
    else:
        decompressed_file_dir = destination_dir

    logger.info(
        "Decompressed zip file",
        data={
            "local_zip_path": str(local_zip_path),
            "decompressed_file_dir": str(decompressed_file_dir),
        },
    )
    return decompressed_file_dir


def process_zip_file(s3_object: S3Object) -> Tuple[LocalDirectoryStore, Path]:
    """
    Download a zip file from S3, decompress it and create a LocalDirectoryStore from the decompressed files.

    Returns:
        LocalDirectoryStore: A local store representing the processed folder.
    """
    # Download the zip file.
    local_object_key = download_zip_file(s3_object=s3_object)

    # Decompress the zip file locally.
    decompressed_file_dir = decompress_zip_file(local_object_key)

    # Validate that the decompressed folder contains files and create LocalDirectoryStore.
    local_store = LocalDirectoryStore(decompressed_file_dir)
    files = local_store.get_file_names()
    if not files:
        err_msg = f"Decompressed directory {decompressed_file_dir} is empty for s3_object_key {s3_object.key}. Available files: {files}"
        raise FileNotFoundError(err_msg)

    logger.info(
        "S3 zip object processed successfully",
        data={
            "s3_object_key": s3_object.key,
            "decompressed_file_dir": str(decompressed_file_dir),
        },
    )
    return local_store, decompressed_file_dir
