import json
import os
from typing import List

from dpytools.logging.logger import DpLogger

logger = DpLogger("data-ingress-pipelines")


def files_size_not_0(files_dir):
    is_file_empty = os.stat(files_dir).st_size == 0
    assert not (is_file_empty), f"{files_dir} is empty"
    logger.info(f"{files_dir} is not empty")


def metadata_json_is_parseable(files_dir):
    try:
        with open(files_dir, "r") as is_files_dir_parseable:
            json.load(is_files_dir_parseable)
        logger.info(f"{files_dir} is parseable")
    except Exception as err:
        raise Exception(f"{files_dir} is not parseable") from err
