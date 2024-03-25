import pytest

import pandas as pd

from dpypelines.pipeline.shared.transforms.validate_transform import (
    number_of_obs_from_xml_file_check,
    check_header_info,
    check_tables_list,
    check_temp_df,
    check_xml_type,
)

def test_number_of_obs_from_xml_file_check_no_obs_found():
    # TODO - create fixture with no 'na_:Obs'
    pass

def test_number_of_obs_from_xml_file_check_obs_numbers_not_matching():
    # TODO - create correct fixture but pass different df_length
    pass

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
    tables = {'key': ''}
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(tables)
        
    assert f"expected 'tables' variable to be a list but got {type(tables)}" in str(err.value)
    
def test_check_tables_list_incorrect_key_value():
    item = ''
    tables = [item]
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(tables)
        
    assert f"expected item of list to be a dict but got {type(item)}" in str(err.value)
    
def test_check_tables_list_missing_obs():
    tables = [{'key1': '', 'key2': ''}]
    
    with pytest.raises(AssertionError) as err:
        check_tables_list(tables)
        
    assert "no observation data found in section of the 'tables' variable" in str(err.value)
        
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