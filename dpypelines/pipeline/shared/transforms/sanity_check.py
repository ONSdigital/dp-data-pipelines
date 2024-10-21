import json
from pathlib import Path
import pandas as pd


def sdmx_sanity_check_v1(sdmx_file: Path):
    """
    Sanity check that the recieved sdmx file is actually an sdmx file.
    """
    # TODO use sdmx_path.suffix == ".xml" here
    assert sdmx_file.name.endswith(".xml"), "Invalid sdmx_file"

    try:
        with open(sdmx_file, "r") as f:
            f.read()
    except Exception as err:
        raise Exception(f"Failed to read in xml - {sdmx_file}") from err


def csv_sanity_check_v1(csv_file: Path):
    """
    Sanity check that the received csv file is actually an sdmx file.
    """
    assert csv_file.suffix == ".csv", "Invalid csv file"

    try:
        pd.read_csv(csv_file)
    except Exception as err:
        raise Exception(f"Failed to read csv file {csv_file}") from err


def json_sanity_check_v1(json_file: Path):
    assert json_file.suffix == ".json"

    try:
        with open(json_file, "r") as f:
            json.load(f)
    except Exception as err:
        raise Exception(f"Failed to read json file {json_file}") from err
