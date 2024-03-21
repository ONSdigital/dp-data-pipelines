import pytest
import pandas as pd

# from dpypelines.pipeline.shared.transforms.sdmx.sdmx_2_0 import xmlToCsvSDMX2_0
from sdmx_2_0 import xmlToCsvSDMX2_0 



def test_xmlToCsvSDMX2_0(input_path, output_path):
    complete_table =  xmlToCsvSDMX2_0(input_path, output_path)

    df_csv = pd.read_csv(output_path)
    df_csv.replace({'Test': {
        False: "false"
    }}, inplace=True)
    assert len(df_csv.columns.values.tolist()) == len(complete_table.columns.values.tolist())
    assert sorted(df_csv.columns.values.tolist()) == sorted(complete_table.columns.values.tolist())
    assert df_csv.shape[1] == complete_table.shape[1]
    assert (df_csv.iloc[0:1, :19]).equals(complete_table.iloc[0:1, :19])
    
def test_xmlToCsvSDMX2_0_assert_errors(input_path, output_path):
    
    with pytest.raises(AssertionError) as err:
        xmlToCsvSDMX2_0(input_path, output_path)
    assert "Length of columns (or shape of column) in tidy csv doesn't match that of input xml", err

# input_path = 'SUT T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_25_Jan_2024 (SDMX 2.0)edited.xml',
# output_path = 'tidy.csv'
test_xmlToCsvSDMX2_0('SUT T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_25_Jan_2024 (SDMX 2.0)edited.xml','tidy.csv')



