# these validations are based off of a limited knowledge of what the final csv will look like and contain
# seems likely that more validation steps will want to be added, extra steps can be added easily into
# the validate_csv() function. If the new validation step checks columns one at a time then add inside
# of the loop within validate_csv


from typing import List, Any
from pathlib import Path
import pandas as pd
import json

def _read_in_csv(csv):
    """
    given a csv will read in first 5 lines as a check
    """
    try:
        df_sample = pd.read_csv(csv, nrows=5)
    except:
        raise Exception(f"Failed to read in csv - {csv}")
        
        
def _read_in_metadata(metadata):
    """
    given a metadata json file attempts to read in a metadata json file
    returns the metadata as a dict if successful
    """
    try:
        with open(metadata, 'r') as f:
            metadata_json = json.load(f)
        return metadata_json
    except:
        raise Exception(f"Failed to read in metadata - {metadata}")
        

def _check_metadata_columns(metadata_json):
    """
    given a metadata json file checks that the column data is where it is expected to be
    """
    try:
        metadata_json['editions'][0]['table_schema']['columns']
    except:
        raise KeyError("['editions'][0]['table_schema']['columns'] not found in metadata file")


def _correct_columns_exist(csv, columns: List[str]):
    """
    given a list of columns, make sure those columns are in the csv
    """
    df = pd.read_csv(csv, nrows=0) # reading in columns only
    
    assert len(df.columns) == len(columns), f"Number of columns in csv file ({len(df.columns)}) does not match len of expected columns ({len(columns)})"
    
    for col in columns:
        assert col in df.columns, f"Expect column {col} not found in csv file"
        
    # number of columns are the same and all expected columns are in df so no
    # need to check the other way around


def _column_has_type(df, column: str, data_type: Any):
    """
    given a df, a column name and a type, make sure that all
    values in that column can be case to that type (int, float, str etc)
    """
    # just a dict to tody up any spelling discrepancies
    type_dict = {
            'string': 'str',
            'bool': 'bool',
            'int': 'int',
            'integer': 'int',
            'float': 'float'
        }
    
    expected_data_type = type_dict.get(data_type.lower(), None) # expected value is from the metadata
    
    if expected_data_type == None:
        raise NotImplementedError(f"Currently have no conversion for data_type {data_type} given from metadata")
    
    observed_data_type = df[column].dtypes # observed value is from the dataframe 
    
    # pandas dataframes store dtypes as numpy dtypes so cannot be directly compared 
    # with regular python dtypes
    # this is checked by finding out if the expected dtype word is found in dataframe dtype word
    # ie is 'int' found in 'numpy.dtypeint64'
    # strings work slightly differently
    if expected_data_type != 'str':
        assert expected_data_type in str(observed_data_type), f"Expected data type for column {column} is given as {data_type} but observed data type from df is {observed_data_type}"
    
    else:
        # pandas df usually stores strings as objects dtype, object dtype also covers multi types
        # so will need to check each value individually
        for value in df[column].unique():
            assert type(value) == str, f"{value} in {column} column should be of type str not {type(value)}"


def _column_has_no_blanks(df, column: str):
    """
    given a df and a column name confirm that there are
    no blank entries in the column.
    """
    # will treat '' & pd.isnull as blanks                 
    for value in df[column].unique():
        if type(value) == bool:
            # bool can only be True of False so not empty
            continue
        elif type(value) == str:
            if not value.strip(): # catches whitespace strings
                raise ValueError(f"{column} has blank entries - '{value}' found")
        else:
            if pd.isnull(value):
                raise ValueError(f"{column} has blank entries - '{value}' found")


def validate_csv(csv_path: Path, metadata_path: Path):
    """
    Validate a csv
    """
    
    # check if csv_path is a csv
    assert csv_path.name.endswith('.csv'), "Invalid csv_path"
    
    # check if metadata_path is a json file
    assert metadata_path.name.endswith('.json'), "Invalid metadata_path"
    
    # check that csv_path can be read in
    _read_in_csv(csv_path)
        
    # same with metadata_path but will read in whole file and keep
    metadata_json = _read_in_metadata(metadata_path)
    
    # column metadata found in expected place in dict
    _check_metadata_columns(metadata_json)
        
    # checking column are as expected 
    expected_columns = {}
    for col in metadata_json['editions'][0]['table_schema']['columns']:
        expected_columns.update({col['name']: col['datatype']})
        
    _correct_columns_exist(csv_path, list(expected_columns))
    
    # validating each column
    # will be reading in column data here to avoid reading in twice per column
    for col in expected_columns:
        df_column = pd.read_csv(csv_path, usecols=[col]) # df with one column
        # checking no blanks before dtype because if a column has an empty value
        # it can change the dtype of the whole column
        _column_has_no_blanks(df_column, col)
        _column_has_type(df_column, col, expected_columns[col])
        

