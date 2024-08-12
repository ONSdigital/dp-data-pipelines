import pytest

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared.pipelineconfig.transform import get_transform_details
from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_sanity_check_v1,
)


def test_get_transform_details_transform_function():
    """
    Checks that get_transform_details successfully retrieves the field 
    containing the transform function from an input config dictionary.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    result = get_transform_details(config, "transform")
    assert result.__name__ == "sdmx_compact_2_0_prototype_1"


def test_get_transform_details_inputs():
    """
    Checks that get_transform_details successfully retrieves the 'transform_inputs'
    field from an input config dictionary.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    result = get_transform_details(config, "transform_inputs")
    assert "^data.xml$" in result.keys()
    assert result["^data.xml$"].__name__ == "sdmx_sanity_check_v1"


def test_get_transform_details_kwargs():
    """
    Checks that get_transform_details successfully retrieves the 'transform_kwargs'
    field from an input config dictionary.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {"kwarg1": "value1"},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    result = get_transform_details(config, "transform_kwargs")
    assert result == {"kwarg1": "value1"}


def test_get_transform_details_missing():
    """
    Checks that get_transform_details returns the expected Assertion error when 
    the given input field does not exist in the config dictionary.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_transform_details(config, "transform_inputs")
    assert "'transform_inputs' not found in config dictionary" in str(err.value)

def test_get_transform_details_invalid_config_version():
    """
    Checks that get_transform_details returns the expected error when the config 
    version in the config dictionary is invalid/not recognised.
    """
    config = {
        "config_version": 2,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(NotImplementedError) as err:
        get_transform_details(config, "transform_inputs")
    assert "Config version 2 not recognised" in str(err.value)