"""
Transform validation functions to be used by dpypelines.pipeline.shared.transforms.sdmx.generic.v21.prototype.v1
ie -  generic sdmx data version 2_1
"""

from pathlib import Path
from typing import List

import pandas as pd


def check_read_in_sdmx(xml_file: Path):
    # check sdmx can be read in
    with open(xml_file, "r") as f:
        xml_content = f.read()
    assert xml_content.startswith("<?xml"), "file does not appear to be xml"


def check_xml_type(data: dict):
    # check that the xml type is 'GenericData'
    assert len(data.keys()) == 1, "xml format looks incorrect"
    assert (
        "message:GenericData" in data.keys()
    ), "could not find 'GenericData' in xml data"


def check_header_info(header: dict):
    # check that this header dict follows a specific schema
    assert isinstance(header, dict), "check_header_info function was expecting a dict"
    expected_keys = [
        "ID",
        "Test",
        "Prepared",
        "Sender",
        "Receiver",
        "Name",
        "Structure",
        "DataSetAction",
        "DataSetID",
        "Extracted",
        "Source",
    ]
    assert len(expected_keys) == len(
        header
    ), f"was expecting header of size ({len(expected_keys)})"
    for key in header.keys():
        assert key in expected_keys, f"{key} is not expected in header"


def get_number_of_obs_from_xml_file(xml_file: Path):
    # counts number of obs from .xml file
    # uses 'na_:Obs' as an identifier that a line has an observation
    with open(xml_file) as f:
        number_of_obs = sum(1 for line in f if "generic:ObsValue" in line)
    assert (
        number_of_obs != 0
    ), "could not count any observations, likely due to incorrect xml format"
    return number_of_obs


def check_header_unpacked(header_dict: dict):
    # checks that the headers have been unpacked correctly
    assert isinstance(header_dict, dict), "function was expecting a dict"
    for key in header_dict:
        assert isinstance(
            header_dict[key], str
        ), "unpacked header_dict is not fully unpacked"


def check_length_of_dataframe_is_expected_length(
    dataframe: pd.DataFrame, no_of_expected_obs: int
):
    # checks if dataframe is the expected length
    # the expected length is the number of obs found using get_number_of_obs_from_xml_file()
    assert (
        len(dataframe) == no_of_expected_obs
    ), f"expected length of dataframe is not {no_of_expected_obs}"


def check_columns_of_dataframes_are_unique(
    obs_columns: pd.Index, header_columns: pd.Index
):
    for column in obs_columns:
        assert (
            column not in header_columns
        ), f"{column} is duplicated in obs_frame and header_frame"


def check_tidy_data_columns(df_columns: pd.Index):
    # check tidy data headers have no “@” or “na:”
    for col in df_columns:
        assert "@" not in col, "@ found in tidy data column name"
        assert "#" not in col, "# found in tidy data column name"
        assert "generic:" not in col, "generic: found in tidy data column name"


def check_obs_dicts_have_same_keys(obs_dict: List):
    # checks each item in obs_dicts has same length and same keys
    first_dict = obs_dict[0]
    for item in obs_dict:
        assert len(first_dict) == len(item), "not all items in obs_dicts are the same length"
        assert first_dict.keys() == item.keys(), "not all keys in obs_dicts are the same"


