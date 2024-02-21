import pytest

import sys
print(sys.path)


from pipelines.pipeline.shared.pipelineconfig.matching import get_supplementary_distribution_patterns
from tests.fixtures.configs import (
    config_unknown_id,
    config_valid_multiple_supplementary_distributions
)


def test_get_supplementary_distribution_patterns_multiple_matches(config_valid_multiple_supplementary_distributions):
    """This test will check if the function does return the number of expected values (in this case one)"""

    results = get_supplementary_distribution_patterns(config_valid_multiple_supplementary_distributions)

    assert len(results) == 3
    assert results[0] == "*.sdmx"
    assert results[1] == "*.xls"
    assert results[2] == "*.csv"


def test_get_supplementary_distribution_patterns_invalid_id(config_unknown_id):
    """This test will check if the function correctly raises for an unknown `$id`"""

    with pytest.raises(ValueError) as err:
        get_supplementary_distribution_patterns(config_unknown_id)

    assert "No supplementary distribution handling for config" in str(err.value)
