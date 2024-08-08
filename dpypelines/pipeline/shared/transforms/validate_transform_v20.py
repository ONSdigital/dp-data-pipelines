"""
Transform validation functions to be used by dpypelines.pipeline.shared.transforms.sdmx.compact.v20.prototype.v1
ie - version 2_0 of compact sdmx data
file may need renaming when further transforms for different sdmx's are created
"""

from pathlib import Path

import pandas as pd


def get_number_of_obs_from_xml_file(xml_file: Path):
    # counts number of obs from .xml file
    # uses 'na_:Obs' as an identifier that a line has an observation
    with open(xml_file) as f:
        number_of_obs = sum(1 for line in f if "na_:Obs" in line)
    assert (
        number_of_obs != 0
    ), "could not count any observations, likely due to incorrect xml format"
    return number_of_obs


def check_header_info(header: dict):
    # could check that this header dict follows a specific schema
    # but is this always the same
    assert isinstance(header, dict), "check_header_info function was expecting a dict"
    expected_keys = [
        "ID",
        "Test",
        "Name",
        "Prepared",
        "Sender",
        "Receiver",
        "KeyFamilyRef",
        "KeyFamilyAgency",
        "DataSetAgency",
        "DataSetID",
        "DataSetAction",
        "Extracted",
        "Source",
    ]
    assert len(expected_keys) == len(
        header
    ), f"was expecting header of size ({len(expected_keys)})"
    for key in header.keys():
        assert key in expected_keys, f"{key} is not expected in header"


def check_header_unpacked(header_dict: dict):
    # checks that the headers have been unpacked correctly
    assert isinstance(header_dict, dict), "function was expecting a dict"
    for key in header_dict:
        assert isinstance(
            header_dict[key], str
        ), "unpacked header_dict is not fully unpacked"


def check_xml_type(data_As_dict: dict):
    # check that the xml type is 'CompactData'
    assert len(data_As_dict.keys()) == 1, "xml format looks incorrect"
    assert (
        "CompactData" in data_As_dict.keys()
    ), "could not find 'CompactData' in xml data; file is not smx v2.0"


def check_read_in_sdmx(xml_file: Path):
    # check sdmx can be read in
    with open(xml_file, "r") as f:
        xml_content = f.read()
    assert xml_content.startswith("<?xml"), "file does not appear to be xml"


def check_tidy_data_columns(df_columns: pd.Index):
    # check tidy data headers have no “@” or “na:”
    for col in df_columns:
        assert "@" not in col, "@ found in tidy data column name"
        assert "na:" not in col, "na: found in tidy data column name"


def check_length_of_dict_is_expected_length(data_dict: dict, no_of_expected_obs: int):
    # checks if each value in a dict are the expected lengths
    # the expected length is the number of obs found using get_number_of_obs_from_xml_file()
    for key in data_dict:
        assert (
            len(data_dict[key]) == no_of_expected_obs
        ), f"expected length of {key} is not {no_of_expected_obs}"


def check_length_of_dataframe_is_expected_length(
    dataframe: pd.DataFrame, no_of_expected_obs: int
):
    # checks if dataframe is the expected length
    # the expected length is the number of obs found using get_number_of_obs_from_xml_file()
    assert (
        len(dataframe) == no_of_expected_obs
    ), f"expected length of dataframe is not {no_of_expected_obs}"


def check_columns_of_dataframes_are_unique(
    series_columns: pd.Index, obs_columns: pd.Index, header_columns: pd.Index
):
    # checks that each column of the 3 dataframes are unique
    # duplicate column names could result in overriding data
    for column in series_columns:
        assert (
            column not in obs_columns
        ), f"{column} is duplicated in series_frame and obs_frame"
        assert (
            column not in header_columns
        ), f"{column} is duplicated in series_frame and header_frame"

    for column in obs_columns:
        assert (
            column not in header_columns
        ), f"{column} is duplicated in obs_frame and header_frame"
