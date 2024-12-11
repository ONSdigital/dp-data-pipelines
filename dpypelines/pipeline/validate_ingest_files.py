import json
import os

from dpytools.logging.logger import DpLogger

logger = DpLogger("data-ingress-pipelines")


def file_size_0(filepath) -> bool:
    return os.stat(filepath).st_size == 0


def metadata_json_is_parseable(filepath) -> bool:
    try:
        with open(filepath, "r") as is_files_dir_parseable:
            json.load(is_files_dir_parseable)
    except Exception:
        return False
    return True
