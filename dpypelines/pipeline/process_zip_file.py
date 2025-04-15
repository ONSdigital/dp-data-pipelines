from datetime import datetime
import os
from pathlib import Path
from typing import Tuple
import zipfile
from dpytools.logging.logger import DpLogger
from dpytools.s3.basic import _get_s3_client, upload_local_file_to_s3
from dpytools.stores.directory.local import LocalDirectoryStore

logger = DpLogger("data-ingress-pipelines")


def download_zip_file(s3_object_name: str) -> str:
    """
    Downloads a zip file from S3 into a local directory and returns the local file path.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)

    # Split s3_object_name on final "/" in case of nested directory structure (e.g. <dir1>/dir2>/input.zip)
    input_dir, input_zip_name = object_key.rsplit("/", maxsplit=1)
    # Create a local directory to store downloaded zip file
    Path(f"/tmp/{input_dir}").mkdir(parents=True, exist_ok=True)

    target_path = f"/tmp/{object_key}"
    logger.info(f'Changed object key from "{object_key}" to "{target_path}"')

    # Download S3 object to local directory
    client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))
    with open(target_path, "wb") as f:
        client.download_fileobj(Bucket=bucket_name, Key=object_key, Fileobj=f)
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


def upload_to_s3_processing_folder(
    s3_object_name: str,
    local_object_key: str,
    decompressed_file_dir: Path,
) -> str:
    """
    Generate "processing" S3 folder name with timestamp and upload decompressed files/copy input zip file to the generated S3 folder.
    """
    bucket_name, s3_object_key = s3_object_name.split("/", maxsplit=1)
    s3_processing_folder = f"processing/{datetime.now().strftime('%y-%m-%dT%H-%M')}-{decompressed_file_dir.parts[-1]}"
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Upload unzipped files to S3 "processing" folder
    for file_path in decompressed_file_dir.rglob("*"):
        # Delete hidden artefacts e.g. .DS_Store
        if file_path.stem.startswith("."):
            os.remove(file_path)
        else:
            s3_processing_object_name = (
                f"{bucket_name}/{s3_processing_folder}/{file_path.name}"
            )
            upload_local_file_to_s3(
                f"{decompressed_file_dir}/{file_path.name}",
                s3_processing_object_name,
                os.environ.get("AWS_PROFILE"),
            )
    logger.info("Decompressed files uploaded to S3 'processing' folder")

    # Copy original zip file from S3 input location to S3 "processing" folder
    s3_client.copy_object(
        Bucket=bucket_name,
        Key=f"{s3_processing_folder}/{local_object_key}".replace("/tmp/", ""),
        CopySource={"Bucket": bucket_name, "Key": s3_object_key},
    )

    # Delete original zip file from S3 input location
    # s3_client.delete_object(Bucket=bucket_name, Key=s3_object_key)
    logger.info(
        "Input zip file copied to S3 'processing' folder and deleted from input folder"
    )
    return s3_processing_folder


def copy_s3_processing_folder_to_destination_folder(
    s3_object_name: str,
    decompressed_file_dir: Path,
    s3_processing_folder: str,
    destination: str,
) -> str:
    """
    Copy all files in S3 "processing" folder to S3 destination folder.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)
    s3_destination_folder = f"{destination}/{datetime.now().strftime('%y-%m-%dT%H-%M')}-{decompressed_file_dir.parts[-1]}"
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Copy unzipped files from S3 "processing" folder to destination folder
    for file_path in decompressed_file_dir.rglob("*"):
        s3_client.copy_object(
            Bucket=bucket_name,
            Key=f"{s3_destination_folder}/{file_path.name}",
            CopySource={
                "Bucket": bucket_name,
                "Key": f"{s3_processing_folder}/{file_path.name}",
            },
        )

    # Copy original zip file from S3 "processing" folder to destination folder
    s3_client.copy_object(
        Bucket=bucket_name,
        Key=f"{s3_destination_folder}/{object_key}",
        CopySource={
            "Bucket": bucket_name,
            "Key": f"{s3_processing_folder}/{object_key}",
        },
    )
    logger.info(
        "Decompressed files and original zip submission copied to S3 'processed' folder",
        data={"s3_processed_folder": s3_destination_folder},
    )

    return s3_destination_folder


def delete_s3_processing_folder(
    s3_object_name: str, decompressed_file_dir: Path, s3_processing_folder: str
) -> None:
    """
    Delete all files from S3 "processing" folder to indicate successful submission.
    """
    bucket_name, object_key = s3_object_name.split("/", maxsplit=1)
    s3_client = _get_s3_client(profile_name=os.environ.get("AWS_PROFILE"))

    # Delete unzipped files from S3 "processing" folder
    for file_path in decompressed_file_dir.rglob("*"):
        s3_client.delete_object(
            Bucket=bucket_name, Key=f"{s3_processing_folder}/{file_path.name}"
        )

    # Delete zip file from S3 "processing" folder
    s3_client.delete_object(
        Bucket=bucket_name, Key=f"{s3_processing_folder}/{object_key}"
    )
    logger.info(
        "Decompressed files and original zip submission deleted from S3 'processing' folder"
    )


def process_zip_file(s3_object_name: str) -> Tuple[LocalDirectoryStore, Path, str]:
    """
    Download a zip file from S3, decompress it, upload decompressed files to S3 'processing' folder and create a LocalDirectoryStore from the decompressed files.

    Returns:
        LocalDirectoryStore: A local store representing the processed folder.
    """
    # Download the zip file.
    local_object_key = download_zip_file(s3_object_name)

    # Decompress the zip file locally.
    decompressed_file_dir = decompress_zip_file(local_object_key)

    # Generate "processing" S3 folder name with timestamp and upload decompressed files
    s3_processing_folder = upload_to_s3_processing_folder(
        s3_object_name, local_object_key, decompressed_file_dir
    )

    # Validate that the decompressed folder contains files and create LocalDirectoryStore.
    local_store = LocalDirectoryStore(decompressed_file_dir)
    files = local_store.get_file_names()
    if not files:
        err_msg = f"Decompressed directory {decompressed_file_dir} is empty for s3_object_name {s3_object_name}. Available files: {files}"
        raise FileNotFoundError(err_msg)

    logger.info(
        "S3 zip object processed successfully",
        data={
            "s3_object_name": s3_object_name,
            "decompressed_file_dir": str(decompressed_file_dir),
        },
    )
    return local_store, decompressed_file_dir, s3_processing_folder
