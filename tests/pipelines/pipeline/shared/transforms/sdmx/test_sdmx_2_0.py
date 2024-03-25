import pytest
import pandas as pd

from dpypelines.pipeline.shared.transforms.sdmx.sdmx_2_0 import xmlToCsvSDMX2_0

def test_xmlToCsvSDMX2_0():
    input_path = 'dpypelines/pipeline/shared/transforms/sdmx/SUT T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_25_Jan_2024_SDMX2.0_edited.xml'
    output_path = 'dpypelines/pipeline/shared/transforms/sdmx/tidy.csv'
    complete_table =  xmlToCsvSDMX2_0(input_path, output_path)

    df_csv = pd.read_csv(output_path)
    df_csv.replace({'Test': {
        False: "false"
    }}, inplace=True)
    assert len(df_csv.columns.values.tolist()) == len(complete_table.columns.values.tolist())
    assert sorted(df_csv.columns.values.tolist()) == sorted(complete_table.columns.values.tolist())
    assert df_csv.shape[1] == complete_table.shape[1]
    assert (df_csv.iloc[0:1, :19]).equals(complete_table.iloc[0:1, :19])
    

def test_xmlToCsvSDMX2_0_assertion_errors():
    with pytest.raises(AssertionError) as err:
        input_path = 'dpypelines/pipeline/shared/transforms/sdmx/SUT T1500 - NATP.ESA10.SU_SDMX Output_BlueBook_25_Jan_2024_SDMX2.0_edited.xml'
        output_path = 'dpypelines/pipeline/shared/transforms/sdmx/tidy.csv'

        complete_table =  xmlToCsvSDMX2_0(input_path, output_path)
        df_csv = pd.read_csv(output_path)
        df_csv.replace({'Test': {
            False: "false"
        }}, inplace=True)
        assert len(df_csv.columns.values.tolist()) == len(complete_table.columns.values.tolist())
        assert sorted(df_csv.columns.values.tolist()) == sorted(complete_table.columns.values.tolist())
        assert df_csv.shape[1] == complete_table.shape[1]
        assert (df_csv.iloc[0:1, :19]).equals(complete_table.iloc[0:1, :19])
    assert f"Number (or shape) of columns in tidy csv does not match expected number (or shape) of columns" in str(err.value)