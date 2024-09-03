import re

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.generic_file_ingress_v1 import generic_file_ingress_v1
from dpypelines.pipeline.shared.transforms.sanity_check import sdmx_sanity_check_v1
from dpypelines.pipeline.shared.transforms.sdmx.v20 import sdmx_compact_2_0_prototype_1
from dpypelines.pipeline.shared.transforms.sdmx.v21 import sdmx_generic_2_1_prototype_1

# Set a regex pattern matching the `source_id` as `CONFIGURATION` dictionary key
# All fields are required in order for a pipeline transform to run successfully
CONFIGURATION = {
    # This is an example of how to set configuration details for a generic source_id - ending with v2_0, v2_1 for sdmx 2.0 and 2.1 respectively, and
    # generic source_id - ending with move for an sdmx file to be moved
    "^.*_compact_sdmx_v2_0$": {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    },
    "^.*_generic_sdmx_v2_1$": {
        "config_version": 1,
        "transform": sdmx_generic_2_1_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    },
    "^.*_move$": {
        "config_version": 1,
        "transform": None,
        "transform_inputs": {},
        "transform_kwargs": {},
        "required_files": [{"matches": "^(?!manifest.json$)", "count": "1"}],
        "supplementary_distributions": {},
        "secondary_function": generic_file_ingress_v1,
    },
}


def get_pipeline_config(source_id: str) -> dict:
    """
    Get pipeline config details for the given source_id
    """
    pipeline_config = (
        None  # hacky way to make global variable and check if it has been populated
    )
    for key in CONFIGURATION.keys():
        if re.match(key, source_id):
            pipeline_config = CONFIGURATION[key]
            break

    if pipeline_config is None:
        raise KeyError(f"{source_id} not matched in CONFIGURATION")
    else:
        return pipeline_config
