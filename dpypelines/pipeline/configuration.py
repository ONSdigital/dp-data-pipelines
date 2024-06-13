import re

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_sanity_check_v1,
)

# Set a regex pattern matching the `dataset_id` as `CONFIGURATION` dictionary key
# All fields are required in order for a pipeline transform to run successfully
CONFIGURATION = {
    # This is an example of how to set configuration details for a specific dataset_id (`cpih` in this case)
    "^cpih$": {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    },
    # Default configuration (regex pattern matches any string of characters of length >= 0)
    # This *always* needs to be the final entry in the dictionary to prevent inadvertent matches
    "^.*$": {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    },
}


def get_source_id(manifest_dict: dict) -> str:
    """
    This function returns the `source_id` form the provided manifest_dict (which is the data in the manifest.json file).
    """
    return manifest_dict["source_id"]


def get_pipeline_config(dataset_id: str) -> dict:
    """
    Get pipeline config details for the given dataset_id
    """
    for key in CONFIGURATION.keys():
        if re.match(key, dataset_id):
            pipeline_config = CONFIGURATION[key]
            break
    return pipeline_config
