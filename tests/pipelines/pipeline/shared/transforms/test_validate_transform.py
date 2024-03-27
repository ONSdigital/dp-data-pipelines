import pytest

from pathlib import Path
import pandas as pd

from dpypelines.pipeline.shared.transforms.validate_transform import (
    number_of_obs_from_xml_file_check,
    check_header_info,
    check_tables_list,
    check_temp_df,
    check_xml_type,
    check_read_in_sdmx,
    get_number_of_series,
    check_tidy_data_columns
)

test_dir = Path(__file__).parents[4]
fixtures_files_dir = Path(test_dir / "fixtures/test-cases")

def test_number_of_obs_from_xml_file_check_no_obs_found():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform_incorrect_obs.xml')
    df_length = 24

    with pytest.raises(AssertionError) as err:
        number_of_obs_from_xml_file_check(fixture_file, df_length)

    assert "could not count any observations, likely due to incorrect xml format" in str(err.value)
    
def test_number_of_obs_from_xml_file_check_obs_numbers_not_matching():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform.xml')
    incorrec_df_length = 10

    with pytest.raises(AssertionError) as err:
        number_of_obs_from_xml_file_check(fixture_file, incorrec_df_length)

    assert "transform error - expected length of data does not match tidy data" in str(err.value)
    
def test_check_header_info_incorrect_input():
    headers = ['testHeader']
    
    with pytest.raises(AssertionError) as err:
        check_header_info(headers)
        
    assert "function was expecting a dict" in str(err.value)

def test_check_header_info_incorrect_size():
    headers = {'testHeader': ''}
    expected_headers = {'ID': '', 'Test': '', 'Name': '', 'Prepared': '', 'Sender': '', 'Receiver': '', 'KeyFamilyRef': '', 'KeyFamilyAgency': '', 'DataSetAgency': '', 'DataSetID': '', 'DataSetAction': '', 'Extracted': '', 'Source': ''}
    
    with pytest.raises(AssertionError) as err:
        check_header_info(headers)
        
    assert f"length of header ({len(headers)}) does not match expected length ({len(expected_headers)})" in str(err.value)
    
def test_check_header_info_incorrect_header():
    incorrect_header = 'testHeader'
    headers = {incorrect_header: '', 'Test': '', 'Name': '', 'Prepared': '', 'Sender': '', 'Receiver': '', 'KeyFamilyRef': '', 'KeyFamilyAgency': '', 'DataSetAgency': '', 'DataSetID': '', 'DataSetAction': '', 'Extracted': '', 'Source': ''}
    
    with pytest.raises(AssertionError) as err:
        check_header_info(headers)
        
    assert f"{incorrect_header} is not expected" in str(err.value)
    
def test_check_tables_list_incorrect_input():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform.xml')
    tables = {'key': ''}
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(fixture_file, tables)
        
    assert f"expected 'tables' variable to be a list but got {type(tables)}" in str(err.value)
    
def test_check_tables_list_incorrect_key_value():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform.xml')
    item = ''
    tables = [item]
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(fixture_file, tables)
        
    assert f"expected item of list to be a dict but got {type(item)}" in str(err.value)
    
def test_check_tables_list_missing_obs():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform.xml')
    tables = [{'key1': '', 'key2': ''}]
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(fixture_file, tables)
        
    assert "no observation data found in section of the 'tables' variable" in str(err.value)

def test_check_tables_list_missing_time_period():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform.xml')
    tables = [{'key1': '', 'na_:Obs': [{'@OBS_VALUE': '0', '@OBS_STATUS': 'A', '@CONF_STATUS': 'F'}]}]
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(fixture_file, tables)
        
    assert "@TIME_PERIOD not found" in str(err.value)
    
def test_check_tables_list_different_number_of_series():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform.xml')
    tables = [{
        'key1': '', 
        'na_:Obs': [{'@TIME_PERIOD': '', '@OBS_VALUE': '', '@OBS_STATUS': '', '@CONF_STATUS': ''}],
        }, {
        'key1': '', 
        'na_:Obs': [{'@TIME_PERIOD': '', '@OBS_VALUE': '', '@OBS_STATUS': '', '@CONF_STATUS': ''}],
        }]
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(fixture_file, tables)
        
    assert "series length from file does not match number of tables" in str(err.value)
        
def test_check_temp_df_empty_df():
    df = pd.DataFrame()
    
    with pytest.raises(AssertionError) as err:
        check_temp_df(df)
        
    assert f"temp_df is returning an empty dataframe" in str(err.value)
    
def test_check_xml_type_incorrect_dict_size():
    data = {'key1': '', 'key2': ''}
    
    with pytest.raises(AssertionError) as err:
        check_xml_type(data)
        
    assert "xml format looks incorrect" in str(err.value)
    
def test_check_xml_type_incorrect_xml_type():
    data = {'GeneralData': ''}
    
    with pytest.raises(AssertionError) as err:
        check_xml_type(data)
        
    assert "could not find 'CompactData' in xml data" in str(err.value)

def test_check_read_in_sdmx_incorrect_file():
    fixture_file = Path(fixtures_files_dir / 'test_validate_csv_data.csv') # any file that isn't an xml
    
    with pytest.raises(AssertionError) as err:
        check_read_in_sdmx(fixture_file)
        
    assert "file does not appear to be xml" in str(err.value)
    
def test_get_number_of_series_zero_series_found():
    fixture_file = Path(fixtures_files_dir / 'test_validate_csv_data.csv') # any file that isn't an xml
    
    with pytest.raises(AssertionError) as err:
        get_number_of_series(fixture_file)
        
    assert "did not find any series in xml" in str(err.value)
    
def test_check_tidy_data_columns_incorrect_columns():
    df_columns = ["@column"]
    
    with pytest.raises(AssertionError) as err:
        check_tidy_data_columns(df_columns)
        
    assert "@ found in tidy data column name" in str(err.value)
    