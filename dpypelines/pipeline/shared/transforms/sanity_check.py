from pathlib import Path
from typing import List

import pandas as pd


def sdmx_sanity_check_v1(sdmx_file: Path):
    """
    Sanity check that the recieved sdmx file is actually an sdmx file.
    """
    assert sdmx_file.name.endswith(".xml"), "Invalid sdmx_file"

    try:
        pd.read_xml(sdmx_file)
    except Exception as err:
        raise Exception(f"Failed to read in xml - {sdmx_file}") from err
    
file = '/Users/davidbailey/Downloads/SUT T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_25_Jan_2024 (SDMX 2.0).xml'
sdmx_sanity_check_v1(Path(file))








