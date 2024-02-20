import re
import pytest
import json

from pathlib import Path

from pipeline.structures.functions.matching import get_supplementary_distribution_patterns

def test_get_supplementary_distribution_patterns_invalid_id():
    """This test will check if the function does flag invalid `$id`"""
    try:

        path_to_file = Path("tests/test-cases/test_json_invalid_no_id.json")

        with path_to_file.open() as jf:
            content = json.load(jf)


        results = get_supplementary_distribution_patterns(content)
    except ValueError:
        pass



def test_get_supplementary_distribution_patterns_valid_id():
    """This test will check if the function does return the number of expected values (in this case one)"""

    path_to_file = Path("tests/test-cases/test_json_valid_no_id.json")

    with path_to_file.open() as jf:
        content = json.load(jf)


    results = get_supplementary_distribution_patterns(content)

    assert len(results) == 1
    assert results[0] == "*.sdmx"


def test_get_supplementary_distribution_patterns_multiple_matches():
    """This test will check if the function does return the number of expected values(in this case multiple)"""

    path_to_file = Path("tests/test-cases/test_json_multiple_matches.json")

    with path_to_file.open() as jf:
        content = json.load(jf)


    results = get_supplementary_distribution_patterns(content)

    assert len(results) == 3
    assert results[0] == "*.sdmx"
    assert results[1] == "*.sfmx"
    assert results[2] == "*.sgmx"