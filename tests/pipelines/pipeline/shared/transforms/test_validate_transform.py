import pytest

from pathlib import Path
import pandas as pd

from dpypelines.pipeline.shared.transforms.validate_transform import (
    get_number_of_obs_from_xml_file,
    check_header_info,
    check_header_unpacked,
    check_xml_type,
    check_read_in_sdmx,
    check_tidy_data_columns,
    check_length_of_dict_is_expected_length,
    check_length_of_dataframe_is_expected_length,
    check_columns_of_dataframes_are_unique,
)

test_dir = Path(__file__).parents[4]
fixtures_files_dir = Path(test_dir / "fixtures/test-cases")

def test_get_number_of_obs_from_xml_file_no_obs_found():
    fixture_file = Path(fixtures_files_dir / 'test_validate_transform_incorrect_obs.xml')

    with pytest.raises(AssertionError) as err:
        get_number_of_obs_from_xml_file(fixture_file)

    assert "could not count any observations, likely due to incorrect xml format" in str(err.value)
    
def test_check_header_info_incorrect_input():
    headers = ['testHeader']
    
    with pytest.raises(AssertionError) as err:
        check_header_info(headers)
        
    assert "check_header_info function was expecting a dict" in str(err.value)

def test_check_header_info_incorrect_size():
    headers = {'testHeader': ''}
    expected_headers = {'ID': '', 'Test': '', 'Name': '', 'Prepared': '', 'Sender': '', 'Receiver': '', 'KeyFamilyRef': '', 'KeyFamilyAgency': '', 'DataSetAgency': '', 'DataSetID': '', 'DataSetAction': '', 'Extracted': '', 'Source': ''}
    
    with pytest.raises(AssertionError) as err:
        check_header_info(headers)
        
    assert f"was expecting header of size ({len(expected_headers)})" in str(err.value)
    
def test_check_header_info_incorrect_header():
    incorrect_header = 'testHeader'
    headers = {incorrect_header: '', 'Test': '', 'Name': '', 'Prepared': '', 'Sender': '', 'Receiver': '', 'KeyFamilyRef': '', 'KeyFamilyAgency': '', 'DataSetAgency': '', 'DataSetID': '', 'DataSetAction': '', 'Extracted': '', 'Source': ''}
    
    with pytest.raises(AssertionError) as err:
        check_header_info(headers)
        
    assert f"{incorrect_header} is not expected" in str(err.value)

def test_check_header_unpacked_not_fully_unpacked():
    not_fully_unpacked_dict = {'Name': {'key1': 'value1'}}

    with pytest.raises(AssertionError) as err:
        check_header_unpacked(not_fully_unpacked_dict)

    assert "unpacked header_dict is not fully unpacked" in str(err.value)
    
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
    
def test_check_tidy_data_columns_incorrect_columns():
    df_columns = ["@column"]
    
    with pytest.raises(AssertionError) as err:
        check_tidy_data_columns(df_columns)
        
    assert "@ found in tidy data column name" in str(err.value)

