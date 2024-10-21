"""
Transform validation functions to be used by dpypelines.pipeline.shared.transforms.csv.join.v1
ie - joining 2 csv files of the same structure
"""

from pathlib import Path
import csv
import pandas as pd

def check_multiple_files(input_files: list):

    assert (
        len(input_files) > 1
    ), "only 1 input file detected, nothing to join"

def check_headers_match(headers: list):

    def equalLists(lists):
        return not lists or all(lists[0] == b for b in lists[1:])
            
    assert (
        equalLists([headers]) == True
    ), "Header mismatch, check input files for matching schema"

def check_length_of_csv_is_as_expected(dataframes:list, combined_csv:pd.DataFrame):

    total_length_of_all_dataframes = 0

    for i in dataframes:
        total_length_of_all_dataframes += len(i)

    assert( len(combined_csv) == total_length_of_all_dataframes
    ), "Total length of each dataframe does not match the length of combined csv"


def check_tidy_data_columns(df_columns: pd.Index):
    # check tidy data headers have no “@” or “na:”
    for col in df_columns:
        assert "@" not in col, "@ found in tidy data column name"
        assert "na:" not in col, "na: found in tidy data column name"

