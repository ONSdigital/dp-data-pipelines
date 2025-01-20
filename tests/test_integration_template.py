from dotenv import load_dotenv
import os
from dpypelines.pipeline.generic_file_ingress_v1 import generic_file_ingress_v1
load_dotenv()

# I've pulled this into this test script but this should sit in a fixture
CONFIGURATION = {
    "valid": {
        "config_version": 1,
        "transform": '',
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.csv"}],
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": '',
    },
}

# This is some example logic for testing 
# class TestMetaData:
#     root_test_data_dir = f"tests/test_data"

#     def test_missing_metadata(self):
#         file_directory = f"{self.root_test_data_dir}/no_metadata"


# root_test_data_dir = f"tests/test_data"
# file_directory = f"{root_test_data_dir}/valid_metadata"
# generic_file_ingress_v1(file_directory , pipeline_config=CONFIGURATION['valid'])



root_test_data_dir = f"tests/test_data"
file_directory = f"{root_test_data_dir}/no_metadata"
generic_file_ingress_v1(file_directory , pipeline_config=CONFIGURATION['valid'])