import json
import os


def file_size_0(filepath, give_error: bool = False) -> bool:
    """
    Checks the given file to ensure it is not empty. If it is, return True.
    give_error argument allows an error to be raised instead.
    """
    if give_error and os.stat(filepath).st_size == 0:
        raise ValueError(f"'{filepath}' is empty")
    else:
        return os.stat(filepath).st_size == 0


def metadata_json_is_parseable(filepath, give_error: bool = False) -> bool:
    """
    Check that the given file can be loaded as json. If not, return False.
    give_error argument allows an error to be raised instead.
    """
    try:
        with open(filepath, "r") as is_files_dir_parseable:
            json.load(is_files_dir_parseable)
    except Exception:
        if give_error:
            raise Exception("metadata.json is not parseable")
        else:
            return False
    return True
