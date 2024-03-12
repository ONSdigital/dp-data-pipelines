# this is a "functions that validate dataframes" toolbox
# will be pieced together in another ticket (delete this text when it gets picked up)
# more functions can be added as needed


from typing import List
from pathlib import Path
import pandas as pd

def _read_in_csv_check(csv_path):
    """
    given a csv will read in first 5 lines as a check
    """
    assert csv_path.name.endswith('.csv'), "Invalid csv_path"
    
    try:
        pd.read_csv(csv_path, nrows=5)
    except:
        raise Exception(f"Failed to read in csv - {csv_path}")
        
        
def _correct_columns_exist(csv_path, columns: List[str]):
    """
    given a list of columns, make sure those columns are in the dataframe
    """
    df = pd.read_csv(csv_path, nrows=0) # reading in columns only
    
    assert len(df.columns) == len(columns), f"Number of columns in csv file ({len(df.columns)}) does not match len of expected columns ({len(columns)})"
    
    for col in columns:
        assert col in df.columns, f"Expect column {col} not found in csv file"
        
    # number of columns are the same and all expected columns are in df so no
    # need to check the other way around


def _dataframe_has_no_blanks(df):
    """
    given a df and a column name confirm that there are
    no blank entries in the column.
    """
    for col in df.columns:
        # will treat '' & pd.isnull as blanks                 
        for value in df[col].unique():
            if type(value) == bool:
                # bool can only be True of False so not empty
                continue
            elif type(value) == str:
                if not value.strip(): # catches whitespace strings
                    raise ValueError(f"{col} has blank entries - '{value}' found")
            else:
                if pd.isnull(value):
                    raise ValueError(f"{col} has blank entries - '{value}' found")
                    

def _dataframe_has_no_duplicates(df):
    """
    given a df confirm that there are no duplicate values
    """
    assert len(df) == len(df.drop_duplicates()), "Found duplicate rows in the dataframe, failed validation"
    

def dataframing_slicing(csv_path):
    """
    given a csv path read in the dataframe as slices and return these slices as a list
    """
    # currently a generic slice size, theres probably a most efficient size
    chunk_size = 5000 
    dataframe_slice_list = []

    for df_slice in pd.read_csv(csv_path, chunksize=chunk_size):
        dataframe_slice_list.append(df_slice)

    return dataframe_slice_list


def validate_csv(csv_path: Path, columns: List[str]):
    """
    Validate a csv
    """
    
    # this function could look something like the below

    # check that csv_path can be read in
    # _read_in_csv_check(csv_path)
    
    # check correct columns in data
    # _dataframe_correct_columns_exist(csv_path, columns)
    
    # get the dataframe as slices
    # df_slice_list = dataframing_slicing(csv_path)
    # for df_slice in df_slice_list:
    #   _dataframe_has_no_duplicates(df_slice)
    #   _dataframe_column_has_no_blanks(df_slice)
        
