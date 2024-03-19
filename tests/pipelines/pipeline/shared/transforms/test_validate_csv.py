import pytest

from pathlib import Path
import pandas as pd

from dpypelines.pipeline.shared.transforms.validate_csv import (
    _read_in_csv_check,
    _correct_columns_exist,
    _dataframe_has_no_blanks,
    _dataframe_has_no_duplicates,
    generated_dataframe_slices,
)

test_dir = Path(__file__).parents[4]
fixtures_files_dir = Path(test_dir / "fixtures/test-cases")

def test_read_in_csv_check_invalid_csv_path():
    csv_path = Path('')
    
    with pytest.raises(AssertionError) as err:
        _read_in_csv_check(csv_path)
        
    assert "Invalid csv_path" in str(err.value)

def test_validate_csv_invalid_file():
    csv_path = Path('data.csv')
    
    with pytest.raises(Exception) as err:
        _read_in_csv_check(csv_path)
        
    assert str(err.value) == f"Failed to read in csv - {csv_path}"

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

def test_dataframe_has_no_blanks_optional_columns_not_a_list():
    optional_columns = 'col1'
    df = pd.DataFrame({'col1': ['1', '2'], 'col2': ['3', '4']})

    with pytest.raises(AssertionError) as err:
        _dataframe_has_no_blanks(df, check_specific_columns=optional_columns)

    assert "check_specific_columns kwarg must be a list" in str(err.value)

def test_dataframe_has_no_blanks_optional_columns_incorrect():
    optional_columns = ['col3']
    df = pd.DataFrame({'col1': ['1', '2'], 'col2': ['3', '4']})

    with pytest.raises(AssertionError) as err:
        _dataframe_has_no_blanks(df, check_specific_columns=optional_columns)

    assert f"{optional_columns[0]} in check_specific_columns not found in dataframe" in str(err.value)
    
def test_dataframe_has_no_blanks_whitespace_entry():
    blank_value = '     '
    df = pd.DataFrame({'col1': ['1', blank_value], 'col2': ['3', '4']})
    column = 'col1'
    
    with pytest.raises(ValueError) as err:
        _dataframe_has_no_blanks(df)
        
    assert str(err.value) == f"{column} has blank entries"

def test_dataframe_has_no_blanks_none_entry():
    blank_value = None
    df = pd.DataFrame({'col1': ['1', blank_value], 'col2': ['3', '4']})
    column = 'col1'
    
    with pytest.raises(ValueError) as err:
        _dataframe_has_no_blanks(df)
        
    assert str(err.value) == f"{column} has blank entries"

def test_dataframe_has_no_duplicates():
    df = pd.DataFrame({'col1': ['1', '2', '1'], 'col2': ['3', '4', '3']}) # rows 1 & 3 the same
    
    with pytest.raises(AssertionError) as err:
        _dataframe_has_no_duplicates(df)
        
    assert "Found duplicate rows in the dataframe, failed validation" in str(err.value)

def test_generated_dataframe_slices():
    fixture_file = Path(fixtures_files_dir / 'test_validate_csv_data.csv')
    expected_columns_from_fixture_file = ['ID', 'TIME_PERIOD', 'OBS_VALUE', 'OBS_STATUS', 'CONF_STATUS']
    chunk_size = 3
    len_fixture_file = 10 # has 10 rows of data
    len_final_slice = len_fixture_file % chunk_size # returns 1
    total_number_of_slices = len_fixture_file // chunk_size # returns 3, will be counting using zero indexing
    count = 0 # used to find final slice
    
    try:
        for df in generated_dataframe_slices(fixture_file, chunk_size=chunk_size):
            assert type(df) == pd.DataFrame, "generated_dataframe_slices not returning a pd.DataFrame"
            assert list(df.columns) == expected_columns_from_fixture_file, "df returing incorrect column headers"
            if count == total_number_of_slices: 
                # if count is 3 then length should be final slice length (1 in this case)
                assert len(df) == len_final_slice, f"final df should have length {len_final_slice} but found {len(df)}"
            else:
                assert len(df) == chunk_size, f"df should have length {chunk_size} but has {len(df)}"

            count += 1
      
    except:
        raise Exception('Unexpected error')