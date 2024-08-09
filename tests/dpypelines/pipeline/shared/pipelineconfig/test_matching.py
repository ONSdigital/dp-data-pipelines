import pytest

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared.pipelineconfig.matching import (
    get_required_files_patterns,
    get_supplementary_distribution_patterns,
)
from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_sanity_check_v1,
)


def test_get_supplementary_distribution_patterns_single_match():
    """
    Ensures that a pipeline config dictionary with one supplementary distribution match returns one result
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
    results = get_supplementary_distribution_patterns(config)

    assert len(results) == 1
    assert set(results) == {"^data.xml$"}


def test_no_supplementary_distributions():
    """
    Ensures that the correct error is raised if the `supplementary_distributions` field is missing from the pipeline config dictionary
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_supplementary_distribution_patterns(config)
    assert "'supplementary_distributions' field not found in config dictionary" in str(
        err.value
    )


def test_get_supplementary_distribution_patterns_multiple_matches():
    """
    Ensures that a pipeline config dictionary with multiple supplementary distribution matches returns three results
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$", "count": "1"}],
        "supplementary_distributions": [
            {"matches": "^data.xml$", "count": "1"},
            {"matches": "^data.csv$", "count": "1"},
            {"matches": "^data.xls$", "count": "1"},
        ],
        "secondary_function": dataset_ingress_v1,
    }
    results = get_supplementary_distribution_patterns(config)

    assert len(results) == 3
    assert set(results) == {"^data.xml$", "^data.csv$", "^data.xls$"}


def test_get_required_files_patterns_single_match():
    """
    Ensures that a pipeline config dictionary with one required file match returns one result
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
    results = get_required_files_patterns(config)

    assert len(results) == 1
    assert set(results) == {"^data.xml$"}


def test_no_required_files():
    """
    Ensures that the correct error is raised if the `required_files` field is missing from the pipeline config dictionary
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_required_files_patterns(config)
    assert "'required_files' field not found in config dictionary" in str(err.value)


def test_get_required_files_patterns_multiple_matches():
    """
    Ensures that a pipeline config dictionary with multiple required files matches returns three results
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.xml$", "count": "1"},
            {"matches": "^data.csv$", "count": "1"},
            {"matches": "^data.xls$", "count": "1"},
        ],
        "supplementary_distributions": [{"matches": "^data.xml$", "count": "1"}],
        "secondary_function": dataset_ingress_v1,
    }
    results = get_required_files_patterns(config)

    assert len(results) == 3
    assert set(results) == {"^data.xml$", "^data.csv$", "^data.xls$"}
