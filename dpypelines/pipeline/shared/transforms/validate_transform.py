
def number_of_obs_from_xml_file_check(file, df_length):   
    # counts number of obs from .xml file
    # uses 'na_:Obs' as an identifier that a line has an observation
    with open(file) as f:
        number_of_obs = sum(1 for line in f if 'na_:Obs' in line)
    assert number_of_obs != 0, "could not count any observations, likely due to incorrect xml format"
    assert number_of_obs == df_length, "transform error - expected length of data does not match tidy data"
    
# TODO - check other xml files
def check_header_info(header):
    # could check that this header dict follows a specific schema
    # but is this always the same
    assert type(header) == dict, "function was expecting a dict"
    expected_keys = ['ID', 'Test', 'Name', 'Prepared', 'Sender', 'Receiver', 'KeyFamilyRef', 'KeyFamilyAgency', 'DataSetAgency', 'DataSetID', 'DataSetAction', 'Extracted', 'Source']
    assert len(expected_keys) == len(header), f"length of header ({len(header)}) does not match expected length ({len(expected_keys)})"
    for key in header.keys():
        assert key in expected_keys, f"{key} is not expected"
    
def check_tables_list(tables_list):
    # check the tables list is a list of dicts
    # each dict is the same size - will this always be the case
    assert type(tables_list) == list, f"expected 'tables' variable to be a list but got {type(tables_list)}" 
    for item in tables_list:
        assert type(item) == dict, f"expected item of list to be a dict but got {type(item)}"
        assert 'na_:Obs' in item.keys(), "no observation data found in section of the 'tables' variable"

def check_temp_df(df):
    # check that temp_df is not empty and is as expected
    assert len(df) != 0, f"temp_df is returning an empty dataframe"

def check_xml_type(data_As_dict):
    # check that the xml type is 'CompactData'
    assert len(data_As_dict.keys()) == 1, "xml format looks incorrect"
    assert 'CompactData' in data_As_dict.keys(), "could not find 'CompactData' in xml data"
    
