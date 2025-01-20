import json
import os

from dpytools.logging.logger import DpLogger
from typing import Optional

logger = DpLogger("data-ingress-pipelines")


def file_size_0(filepath, give_error: Optional[bool]) -> bool:
    """
    Checks the given file to ensure it is not empty. If it is, return True.
    Optional variable allows an error to be raised instead.
    """
    if give_error and os.stat(filepath).st_size == 0:
        err = ValueError(f"'{filepath}' is empty")
        logger.error("Input file is empty", err, data={"file": filepath})
        raise err
    else:
        return os.stat(filepath).st_size == 0


def metadata_json_is_parseable(filepath, give_error: Optional[bool]) -> bool:
    """
    Check that the given file can be loaded as json. If not, return False.
    Optional variable allows an error to be raised instead.
    """
    try:
        with open(filepath, "r") as is_files_dir_parseable:
            json.load(is_files_dir_parseable)
            logger.info(
                "metadata.json is parseable as JSON", data={"file": filepath}
                )
    except Exception:
        if give_error:
            raise Exception("metadata.json is not parseable")
        else:
            return False
    return True
