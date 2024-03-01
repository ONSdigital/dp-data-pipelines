import pytest

from pipelines.pipeline.shared.pipelineconfig.matching import (
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


def test_get_supplementary_distribution_patterns_valid_id(config_valid_id):
    results = get_supplementary_distribution_patterns(config_valid_id)

    assert len(results) == 1
    assert set(results) == {"*.sdmx"}


def test_no_supplementary_distributions(config_no_supplementary_distributions):
    with pytest.raises(AssertionError) as err:
        get_supplementary_distribution_patterns(config_no_supplementary_distributions)
    assert "'supplementary_distributions' field not found in config dictionary" in str(
        err.value
    )


def test_get_supplementary_distribution_patterns_multiple_matches(
    config_valid_multiple_matches,
):
    """This test will check if the function does return the number of expected values (in this case one)"""

    results = get_supplementary_distribution_patterns(config_valid_multiple_matches)

    assert len(results) == 3
    assert set(results) == {"*.sdmx", "*.csv", "*.xls"}


def test_get_supplementary_distribution_patterns_invalid_id(config_invalid_id):
    """This test will check if the function correctly raises for an unknown `$id`"""

    with pytest.raises(NotImplementedError) as err:
        get_supplementary_distribution_patterns(config_invalid_id)

    assert (
        "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/invalid.json is not a recognised schema."
        in str(err.value)
    )


def test_get_required_files_patterns_valid_id(config_valid_id):
    results = get_required_files_patterns(config_valid_id)

    assert len(results) == 1
    assert set(results) == {"*.sdmx"}


def test_no_required_files(config_no_required_files):
    with pytest.raises(AssertionError) as err:
        get_required_files_patterns(config_no_required_files)
    assert "'required_files' field not found in config dictionary" in str(err.value)


def test_get_required_files_patterns_multiple_matches(
    config_valid_multiple_matches,
):
    """This test will check if the function does return the number of expected values (in this case one)"""

    results = get_required_files_patterns(config_valid_multiple_matches)

    assert len(results) == 3
    assert set(results) == {"*.sdmx", "*.csv", "*.xls"}


def test_get_required_files_patterns_invalid_id(config_invalid_id):
    """This test will check if the function correctly raises for an unknown `$id`"""

    with pytest.raises(NotImplementedError) as err:
        get_required_files_patterns(config_invalid_id)

    assert (
        "https://raw.githubusercontent.com/ONSdigital/dp-data-pipelines/sandbox/schemas/dataset-ingress/config/invalid.json is not a recognised schema."
        in str(err.value)
    )
