import pytest

from dpypelines.pipeline.dataset_ingress_v1 import dataset_ingress_v1
from dpypelines.pipeline.shared.pipelineconfig.matching import get_matching_pattern
from dpypelines.pipeline.shared.transforms.sdmx.v1 import (
    sdmx_compact_2_0_prototype_1,
    sdmx_sanity_check_v1,
)


def test_get_matching_pattern_multiple_matches():
    """
    Ensures the correct results are returned if get_matching_pattern is run on an input configuration with multiple matches for the given pattern.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [
            {"matches": "^data.xml$"},
            {"matches": "^data.csv$"},
            {"matches": "^data.xls$"},
        ],
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": dataset_ingress_v1,
    }
    results = get_matching_pattern(config, "required_files")

    assert len(results) == 3
    assert set(results) == {"^data.xml$", "^data.csv$", "^data.xls$"}


def test_get_matching_no_match():
    """
    Ensures that the correct error is raised if the specified field, in this case "required_files" is missing from the pipeline config dictionary.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": dataset_ingress_v1,
    }
    with pytest.raises(AssertionError) as err:
        get_matching_pattern(config, "required_files")
    assert "'required_files' field not found in config dictionary" in str(err.value)


def test_get_matching_pattern_single_match():
    """
    Ensures the correct results are returned if get_matching_pattern is run on an input configuration with one match for the given pattern.
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$"}],
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": dataset_ingress_v1,
    }
    results = get_matching_pattern(config, "required_files")

    assert len(results) == 1
    assert set(results) == {"^data.xml$"}


def test_get_matching_pattern_supplementary_distributions():
    """
    Ensures get_matching_pattern can correctly return expected matched results from a config when the given pattern is "supplementary_distributions".
    """
    config = {
        "config_version": 1,
        "transform": sdmx_compact_2_0_prototype_1,
        "transform_inputs": {"^data.xml$": sdmx_sanity_check_v1},
        "transform_kwargs": {},
        "required_files": [{"matches": "^data.xml$"}],
        "supplementary_distributions": [{"matches": "^data.xml$"}],
        "secondary_function": dataset_ingress_v1,
    }
    results = get_matching_pattern(config, "supplementary_distributions")

    assert len(results) == 1
    assert set(results) == {"^data.xml$"}
