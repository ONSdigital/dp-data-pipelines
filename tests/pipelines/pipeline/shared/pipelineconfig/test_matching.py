import pytest

from dpypelines.pipeline.shared.pipelineconfig.matching import (
    get_supplementary_distribution_patterns,
    get_required_files_patterns,
)
from tests.fixtures.configs import (
    config_valid_id,
    config_invalid_id,
    config_valid_multiple_matches,
    config_no_required_files,
    config_no_supplementary_distributions,
)


def test_get_supplementary_distribution_patterns_single_match(config_valid_id):
    """
    Ensures that a config file with one match returns one result
    """
    results = get_supplementary_distribution_patterns(config_valid_id)

    assert len(results) == 1
    assert set(results) == {"^data.sdmx$"}


def test_no_supplementary_distributions(config_no_supplementary_distributions):
    """
    Ensures that the correct error is raised if the `supplementary_distributions` field is missing from the config file
    """
    with pytest.raises(AssertionError) as err:
        get_supplementary_distribution_patterns(config_no_supplementary_distributions)
    assert "'supplementary_distributions' field not found in config dictionary" in str(
        err.value
    )


def test_get_supplementary_distribution_patterns_multiple_matches(
    config_valid_multiple_matches,
):
    """
    Ensures that a config file with multiple matches returns three results
    """
    results = get_supplementary_distribution_patterns(config_valid_multiple_matches)

    assert len(results) == 3
    assert set(results) == {"*.sdmx", "*.csv", "*.xls"}


def test_get_supplementary_distribution_patterns_invalid_id(config_invalid_id):
    """
    Ensures that the correct error is raised for an invalid `$id` value in the config file
    """
    with pytest.raises(NotImplementedError) as err:
        get_supplementary_distribution_patterns(config_invalid_id)

    assert (
        "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/invalid.json is not a recognised schema."
        in str(err.value)
    )


def test_get_required_files_patterns_single_match(config_valid_id):
    """
    Ensures that a config file with one match returns one result
    """
    results = get_required_files_patterns(config_valid_id)

    assert len(results) == 1
    assert set(results) == {"^data.sdmx$"}


def test_no_required_files(config_no_required_files):
    """
    Ensures that the correct error is raised if the `required_files` field is missing from the config file
    """
    with pytest.raises(AssertionError) as err:
        get_required_files_patterns(config_no_required_files)
    assert "'required_files' field not found in config dictionary" in str(err.value)


def test_get_required_files_patterns_multiple_matches(
    config_valid_multiple_matches,
):
    """
    Ensures that a config file with multiple matches returns three results
    """
    results = get_required_files_patterns(config_valid_multiple_matches)

    assert len(results) == 3
    assert set(results) == {"*.sdmx", "*.csv", "*.xls"}


def test_get_required_files_patterns_invalid_id(config_invalid_id):
    """
    Ensures that the correct error is raised for an invalid `$id` value in the config file
    """
    with pytest.raises(NotImplementedError) as err:
        get_required_files_patterns(config_invalid_id)

    assert (
        "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/invalid.json is not a recognised schema."
        in str(err.value)
    )
