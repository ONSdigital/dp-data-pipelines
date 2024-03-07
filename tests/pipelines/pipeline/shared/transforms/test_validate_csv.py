import pytest

from pathlib import Path
import pandas as pd

from pipelines.pipeline.shared.transforms.validate_csv import (
    _read_in_csv,
    _read_in_metadata,
    _check_metadata_columns,
    _correct_columns_exist,
    _column_has_type,
    _column_has_no_blanks,
    validate_csv
)

test_dir = Path(__file__).parents[4]
fixtures_files_dir = Path(test_dir / "fixtures/test-cases")

def test_validate_csv_invalid_csv_path():
    csv_path = Path('')
    metadata_path = Path('')
    
    with pytest.raises(AssertionError) as err:
        validate_csv(csv_path, metadata_path)
        
    assert "Invalid csv_path" in str(err.value)

def test_validate_csv_invalid_matadata_path():
    csv_path = Path('data.csv')
    metadata_path = Path('')
    
    with pytest.raises(AssertionError) as err:
        validate_csv(csv_path, metadata_path)
        
    assert "Invalid metadata_path" in str(err.value)

def test_validate_csv_invalid_file():
    csv_path = Path('data.csv')
    
    with pytest.raises(Exception) as err:
        _read_in_csv(csv_path)
        
    assert str(err.value) == f"Failed to read in csv - {csv_path}"
    
def test_validate_metadata_invalid_file():
    metadata_path = Path('metadata.json')
    
    with pytest.raises(Exception) as err:
        _read_in_metadata(metadata_path)
        
    assert str(err.value) == f"Failed to read in metadata - {metadata_path}"

def test_check_metadata_columns_invalid_format():
    metadata_json = {'key': 'value'}
    
    with pytest.raises(KeyError) as err:
        _check_metadata_columns(metadata_json)
        
    assert "['editions'][0]['table_schema']['columns'] not found in metadata file" in str(err.value)

def test_correct_columns_exist_incorrect_number_of_columns():
    fixture_file = Path(fixtures_files_dir / 'test_validate_csv_data.csv')
    columns = ['OBS_VALUE', 'OBS_STATUS']
    expected_len = 5 # fixture file is a small file with only 5 columns
    
    with pytest.raises(AssertionError) as err:
        _correct_columns_exist(fixture_file, columns)
        
    assert f"Number of columns in csv file ({expected_len}) does not match len of expected columns ({len(columns)})" in str(err.value)
    
def test_correct_columns_exist_different_column_name():
    fixture_file = Path(fixtures_files_dir / 'test_validate_csv_data.csv')
    incorrect_column = 'Incorrect_column_name'
    columns = ['ID', 'TIME_PERIOD', 'OBS_VALUE', 'OBS_STATUS', incorrect_column]
    
    with pytest.raises(AssertionError) as err:
        _correct_columns_exist(fixture_file, columns)
        
    assert f"Expect column {incorrect_column} not found in csv file" in str(err.value)
    
def test_column_has_type_unexpected_data_type():
    df = pd.DataFrame({'col1': ['a', 'b'], 'col2': ['c', 'd']})
    column = 'col1'
    data_type = 'boolean'
    
    with pytest.raises(NotImplementedError) as err:
        _column_has_type(df, column, data_type)
        
    assert str(err.value) == f"Currently have no conversion for data_type {data_type} given from metadata"
    
def test_column_has_type_unmatching_data_types():
    df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
    column = 'col1'
    data_type = 'bool'
    observed_data_type = 'int64'
    
    with pytest.raises(AssertionError) as err:
        _column_has_type(df, column, data_type)
        
    assert f"Expected data type for column {column} is given as {data_type} but observed data type from df is {observed_data_type}" in str(err.value)

def test_column_has_type_incorrect_str_value():
    df = pd.DataFrame({'col1': ['1', True], 'col2': ['3', '4']})
    column = 'col1'
    data_type = 'string'
    value = True
    
    with pytest.raises(AssertionError) as err:
        _column_has_type(df, column, data_type)
        
    assert f"{value} in {column} column should be of type str not {type(value)}" in str(err.value)

def test_column_has_no_blanks_whitespace_entry():
    blank_value = '     '
    df = pd.DataFrame({'col1': ['1', blank_value], 'col2': ['3', '4']})
    column = 'col1'
    
    with pytest.raises(ValueError) as err:
        _column_has_no_blanks(df, column)
        
    assert str(err.value) == f"{column} has blank entries - '{blank_value}' found"

def test_column_has_no_blanks_none_entry():
    blank_value = None
    df = pd.DataFrame({'col1': ['1', blank_value], 'col2': ['3', '4']})
    column = 'col1'
    
    with pytest.raises(ValueError) as err:
        _column_has_no_blanks(df, column)
        
    assert str(err.value) == f"{column} has blank entries - '{blank_value}' found"

def test_validate_csv():
    fixture_csv_file = Path(fixtures_files_dir / 'test_validate_csv_data.csv')
    fixture_metadata_file = Path(fixtures_files_dir / 'test_validate_csv_metadata.json')
    
    try:
        validate_csv(fixture_csv_file, fixture_metadata_file)
    except:
        raise Exception('Unexpected error')
