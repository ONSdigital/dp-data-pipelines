# this is a "functions that validate dataframes" toolbox
# will be pieced together in another ticket (delete this text when it gets picked up)
# more functions can be added as needed


from typing import List, Optional
from pathlib import Path
import pandas as pd

def _read_in_csv_check(csv_path: Path):
    """
    given a csv will read in first 5 lines as a check
    """
    assert csv_path.name.endswith('.csv'), "Invalid csv_path"
    
    try:
        pd.read_csv(csv_path, nrows=5)
    except Exception as err:
        raise Exception(f"Failed to read in csv - {csv_path}") from err
        
        
def _correct_columns_exist(csv_path: Path, columns: List[str]):
    """
    given a list of columns, make sure those columns are in the dataframe
    """
    df = pd.read_csv(csv_path, nrows=0) # reading in columns only
    
    assert len(df.columns) == len(columns), f"Number of columns in csv file ({len(df.columns)}) does not match len of expected columns ({len(columns)})"
    
    for col in columns:
        assert col in df.columns, f"Expect column {col} not found in csv file"
        
    # number of columns are the same and all expected columns are in df so no
    # need to check the other way around


def _dataframe_has_no_blanks(df: pd.DataFrame, check_specific_columns: Optional[List] = None):
    """
    given a df confirm that there are no blank entries in any column
    if check_specific_columns is given as a kwarg then only check the given columns 
    """
    columns_to_check = []
    if check_specific_columns == None:
        columns_to_check = list(df.columns)
    else:
        assert type(check_specific_columns) == list, "check_specific_columns kwarg must be a list"
        
        # confirm all columns in check_specific_columns are in df
        for col in check_specific_columns:
            assert col in df.columns, f"{col} in check_specific_columns not found in dataframe"
            
        columns_to_check = check_specific_columns
        
    for col in columns_to_check:
        # will treat '' & pd.isnull as blanks                 
        for value in df[col].unique():
            if type(value) == bool:
                # bool can only be True of False so not empty
                continue
            elif type(value) == str:
                if not value.strip(): # catches whitespace strings
                    raise ValueError(f"{col} has blank entries")
            else:
                if pd.isnull(value):
                    raise ValueError(f"{col} has blank entries")
                    

def _dataframe_has_no_duplicates(df: pd.DataFrame):
    """
    given a df confirm that there are no duplicate values
    """
    assert len(df) == len(df.drop_duplicates()), "Found duplicate rows in the dataframe, failed validation"


def generated_dataframe_slices(csv_path: Path, chunk_size: Optional[int] = 5000):
    '''
    is a generator function - can iterate through slices of the dataframe
    this way only one slice at a time will be in the memory
    chunk_size is an optional kwarg which will determine the size of each slice
    '''
    
    # count the number of rows in csv
    with open(csv_path) as f:
        # -1 because header should not be counted as a row of data
        len_of_data = sum(1 for line in f) - 1 
    
    # get datafram column names
    df_headers = list(pd.read_csv(csv_path, nrows=0).columns)
    
    # creating the row numbers for the start of each slice
    row_number = 0
    iterable_row_numbers = []
    while row_number <= len_of_data:
        iterable_row_numbers.append(row_number)
        row_number += chunk_size
    
    # creating the generator
    for skip_row_number in iterable_row_numbers:
        if skip_row_number == 0:
            # pandas will read in column names correctly
            df_slice = pd.read_csv(csv_path, nrows=chunk_size)
        else:
            # pandas does not read in column names when using skiprows
            df_slice = pd.read_csv(csv_path, nrows=chunk_size, skiprows=skip_row_number, names=df_headers)
        yield df_slice
    

def validate_csv(csv_path: Path, columns: List[str]):
    """
    Validate a csv
    """
    
    # this function could look something like the below

    # check that csv_path can be read in
    # _read_in_csv_check(csv_path)
    
    # check correct columns in data
    # _dataframe_correct_columns_exist(csv_path, columns)
    
    # validate of the dataframe as slices
    # for df_slice in generated_dataframe_slices(csv_path)
    #   _dataframe_has_no_duplicates(df_slice)
    #   _dataframe_column_has_no_blanks(df_slice)
        