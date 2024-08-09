import pytest

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared.pipelineconfig.transform import (
    get_transform_function,
    get_transform_inputs,
    get_transform_kwargs,
)
from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_sanity_check_v1,
)


def test_get_transform_function():
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    result = get_transform_function(config)
    assert result.__name__ == "sdmx_compact_2_0_prototype_1"


def test_get_transform_function_missing():
    config = {
        "config_version": 1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_transform_function(config)
    assert "'transform' not found in config dictionary" in str(err.value)


def test_get_transform_function_invalid_config_version():
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
        get_transform_function(config)
    assert "Config version 2 not recognised" in str(err.value)


def test_get_transform_inputs():
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    result = get_transform_inputs(config)
    assert "^data.xml$" in result.keys()
    assert result["^data.xml$"].__name__ == "sdmx_sanity_check_v1"


def test_get_transform_inputs_missing():
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_transform_inputs(config)
    assert "'transform_inputs' not found in config dictionary" in str(err.value)


def test_get_transform_inputs_invalid_config_version():
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
        get_transform_inputs(config)
    assert "Config version 2 not recognised" in str(err.value)


def test_get_transform_kwargs():
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {"kwarg1": "value1"},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    result = get_transform_kwargs(config)
    assert result == {"kwarg1": "value1"}


def test_get_transform_kwargs_missing():
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_transform_kwargs(config)
    assert "'transform_kwargs' not found in config dictionary" in str(err.value)


def test_get_transform_kwargs_invalid_config_version():
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
        get_transform_kwargs(config)
    assert "Config version 2 not recognised" in str(err.value)
