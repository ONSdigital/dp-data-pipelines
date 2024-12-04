import json
import os

from dpytools.logging.logger import DpLogger

logger = DpLogger("data-ingress-pipelines")


def file_size_not_0(filepath):
    is_file_empty = os.stat(filepath).st_size == 0
    assert not (is_file_empty), f"{filepath} is empty"
    logger.info(f"{filepath} is not empty")


def metadata_json_is_parseable(filepath):
    try:
        with open(filepath, "r") as is_files_dir_parseable:
            json.load(is_files_dir_parseable)
        logger.info(f"{filepath} is parseable")
    except Exception as err:
        raise Exception(f"{filepath} is not parseable") from err
